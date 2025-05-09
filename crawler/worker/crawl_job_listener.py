import os
import sys
import logging
import time
import signal
from datetime import datetime
import uuid

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR, SCRAPY_PATH, DB_URI, MONGO_CRAWL_JOB_COLLECTION, BEANSTALKD_CRAWL_TUBE
from lib.storage.mongodb_client import MongoDBClient
from crawler.worker.crawl_job_processor import CrawlJobProcessor

class CrawlJobListener:
    """
    Single-process queue listener that handles job setup, environment, post-processing,
    and runs the Scrapy spider directly. Logs results to MongoDB.
    """
    def __init__(self, queue_host=QUEUE_HOST, queue_port=QUEUE_PORT, instance_id=0):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.instance_id = instance_id
        self.running = False
        self.shutdown_requested = False
        self.current_job_id = None
        self.logger = self._setup_logging()
        self.queue_manager = None
        self.mongodb_client = None
        self.crawl_job_processor = None
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_logging(self):
        logger = logging.getLogger(f"CrawlJobListener_{self.instance_id}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:  # Only add handlers if none exist
            os.makedirs(LOG_DIR, exist_ok=True)
            log_file = os.path.join(LOG_DIR, f"crawl_job_listener_{self.instance_id}.log")
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            # Do NOT add a StreamHandler to avoid duplicate logs
        return logger

    def _handle_signal(self, signum, frame):
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        self.logger.info(f"Received {signal_name} signal")
        self.shutdown_requested = True

    def _initialize_components(self) -> bool:
        """
        Initialize queue manager, MongoDB client, and job processor.
        """
        try:
            self.logger.info("Initializing components")
            self.queue_manager = QueueManager(host=self.queue_host, port=self.queue_port)
            self.mongodb_client = MongoDBClient(uri=DB_URI)
            self.crawl_job_processor = CrawlJobProcessor(
                logger=self.logger,
                mongodb_client=self.mongodb_client,
                scrapy_path=SCRAPY_PATH,
                log_dir=LOG_DIR,
                mongo_collection=MONGO_CRAWL_JOB_COLLECTION
            )
            self.logger.info("Components initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False

    def _reserve_job(self, tubes=None, timeout=None):
        if tubes is None:
            tubes = [BEANSTALKD_CRAWL_TUBE]
        try:
            for tube in tubes:
                self.queue_manager.client.watch_tube(tube)
            job_id, job_data = self.queue_manager.dequeue_job(timeout=timeout)
            if job_id:
                self.logger.info(f"Reserved job {job_id}")
                return job_id, job_data, None
            return None, None, None
        except Exception as e:
            self.logger.error(f"Error reserving job: {str(e)}")
            return None, None, e

    def start(self) -> bool:
        """
        Start the crawler queue listener main loop.
        """
        self.logger.info("Starting crawler queue listener (single-process mode)")
        if not self._initialize_components():
            self.logger.error("Failed to initialize components, not started")
            return False
        self.running = True
        while self.running and not self.shutdown_requested:
            try:
                tubes = [BEANSTALKD_CRAWL_TUBE]
                for tube in tubes:
                    self.queue_manager.client.watch_tube(tube)
                try:
                    job_id, job_data, job_obj = self.queue_manager.dequeue_job(tubes=tubes, timeout=5)
                    if job_id:
                        # Ensure a unique crawl_id is present
                        if 'crawl_id' not in job_data or not job_data['crawl_id']:
                            job_data['crawl_id'] = str(uuid.uuid4())
                        crawl_id = job_data['crawl_id']
                        self.logger.info(f"Processing job {job_id} (crawl_id={crawl_id})")
                        # Immediately delete the job from Beanstalkd to prevent double-processing
                        if job_obj:
                            self.queue_manager.delete_job(job_obj)
                        # Update MongoDB: set crawl_status to 'crawling' and update crawl_id
                        try:
                            self.mongodb_client.update_one(
                                MONGO_CRAWL_JOB_COLLECTION,
                                {'crawl_id': crawl_id},
                                {'$set': {'crawl_id': crawl_id, 'crawl_status': 'crawling', 'updated_at': datetime.utcnow()}}
                            )
                            self.logger.info(f"Set crawl_status to 'crawling' for crawl_id {crawl_id}")
                        except Exception as e:
                            self.logger.error(f"Failed to update crawl_status for crawl_id {crawl_id}: {str(e)}")
                        success = self.crawl_job_processor.process_job(job_id, job_data)
                        if success:
                            self.logger.info(f"Job {job_id} (crawl_id={crawl_id}) completed successfully")
                        else:
                            self.logger.warning(f"Failed to process job {job_id} (crawl_id={crawl_id}), marking as failed in MongoDB")
                            # Update MongoDB crawl_status to 'failed'
                            try:
                                self.mongodb_client.update_one(
                                    MONGO_CRAWL_JOB_COLLECTION,
                                    {'crawl_id': crawl_id},
                                    {'$set': {'crawl_status': 'failed', 'updated_at': datetime.utcnow()}}
                                )
                                self.logger.info(f"Set crawl_status to 'failed' for crawl_id {crawl_id}")
                            except Exception as e:
                                self.logger.error(f"Failed to update crawl_status to 'failed' for crawl_id {crawl_id}: {str(e)}")
                except Exception as e:
                    self.logger.error(f"Error in job processing: {str(e)}")
                    time.sleep(5)
            except Exception as e:
                self.logger.error(f"Unhandled exception in main loop: {str(e)}")
                time.sleep(5)
        self.logger.info("Shutting down")
        self.cleanup()

    def cleanup(self):
        self.logger.info("Cleaning up resources")
        if self.queue_manager and hasattr(self.queue_manager, 'close'):
            self.queue_manager.close()
        if self.mongodb_client:
            try:
                self.mongodb_client.close()
            except Exception:
                pass

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Single-process crawler queue listener')
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Queue host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Queue port')
    parser.add_argument('--instance-id', type=int, default=0, help='Worker instance ID')
    args = parser.parse_args()
    worker = CrawlJobListener(
        queue_host=args.queue_host,
        queue_port=args.queue_port,
        instance_id=args.instance_id
    )
    try:
        worker.start()
    except KeyboardInterrupt:
        worker.shutdown_requested = True
        worker.cleanup()

if __name__ == '__main__':
    main()