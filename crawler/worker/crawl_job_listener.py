import os
import sys
import logging
import time
import signal
from datetime import datetime
import uuid
import threading

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from lib.queue.queue_manager import QueueManager
from config.base_settings import (
    QUEUE_HOST,
    QUEUE_PORT,
    LOG_DIR,
    SCRAPY_PATH,
    MONGO_CRAWL_JOB_COLLECTION,
    QUEUE_CRAWL_TUBE,
    QUEUE_TTR,
)
from lib.storage.mongodb_client import MongoDBClient
from crawler.worker.crawl_job_processor import CrawlJobProcessor


class CrawlJobListener:
    """
    Single-process queue listener that handles job setup, environment, post-processing,
    and runs the Scrapy spider directly. Logs results to MongoDB.
    """

    def __init__(self, instance_id=0):
        self.queue_host = QUEUE_HOST
        self.queue_port = QUEUE_PORT
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
        self.max_retries = 3

    def _setup_logging(self):
        logger = logging.getLogger(f"CrawlJobListener_{self.instance_id}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:  # Only add handlers if none exist
            os.makedirs(LOG_DIR, exist_ok=True)
            log_file = os.path.join(
                LOG_DIR, f"crawl_job_listener_{self.instance_id}.log"
            )
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            # Do NOT add a StreamHandler to avoid duplicate logs
        return logger

    def _handle_signal(self, signum, frame):
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        self.logger.info(f"Received {signal_name} signal")
        self.shutdown_requested = True

    def _initialize_components(self) -> bool:
        """
        Initialize queue manager, MongoDB client, and job processor.
        """
        try:
            self.logger.info("Initializing components")
            self.queue_manager = QueueManager(
                host=self.queue_host, port=self.queue_port
            )
            self.mongodb_client = MongoDBClient()
            self.crawl_job_processor = CrawlJobProcessor(
                logger=self.logger,
                mongodb_client=self.mongodb_client,
                scrapy_path=SCRAPY_PATH,
                log_dir=LOG_DIR,
                mongo_collection=MONGO_CRAWL_JOB_COLLECTION,
            )
            self.logger.info("Components initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False

    def _reserve_job(self, tubes=None, timeout=None):
        if tubes is None:
            tubes = [QUEUE_CRAWL_TUBE]
        try:
            job_id, job_data, job_obj = self.queue_manager.dequeue_job(
                tubes=tubes, timeout=timeout
            )
            if job_id:
                self.logger.info(f"Reserved job {job_id}")
                return job_id, job_data, job_obj
            return None, None, None
        except Exception as e:
            self.logger.error(f"Error reserving job: {str(e)}")
            return None, None, e

    def start(self) -> bool:
        """
        Start the crawler queue listener main loop.
        """
        self.logger.info(
            f"Starting crawler queue listener (single-process mode) with MAX_RETRIES={self.max_retries}"
        )
        if not self._initialize_components():
            self.logger.error("Failed to initialize components, not started")
            return False
        self.running = True

        TOUCH_INTERVAL_FACTOR = 0.5
        DEFAULT_JOB_TTR = getattr(
            self.queue_manager.client.connection, "default_ttr", QUEUE_TTR
        )
        MIN_TTR_FOR_TOUCHING = max(30, DEFAULT_JOB_TTR * 0.2)

        while self.running and not self.shutdown_requested:
            job_id = None
            job_data = None
            job_obj = None
            crawl_id = "N/A_BEFORE_DEQUEUE"

            toucher_thread = None
            toucher_active_event = threading.Event()

            try:
                tubes = [QUEUE_CRAWL_TUBE]
                job_id, job_data, job_obj = self._reserve_job(tubes=tubes, timeout=5)

                if job_id and job_data and job_obj:
                    if "crawl_id" not in job_data or not job_data["crawl_id"]:
                        job_data["crawl_id"] = str(uuid.uuid4())
                    crawl_id = job_data["crawl_id"]
                    self.logger.info(
                        f"DEQUEUED job {job_id} (crawl_id={crawl_id}). Job data keys: {list(job_data.keys())}"
                    )

                    job_stats_for_ttr = self.queue_manager.get_job_stats(job_obj)
                    current_job_actual_ttr = (
                        job_stats_for_ttr.get("ttr")
                        if job_stats_for_ttr
                        else DEFAULT_JOB_TTR
                    )
                    self.logger.info(
                        f"Job {job_id} has TTR: {current_job_actual_ttr}s (from stats or default). Min TTR for touching: {MIN_TTR_FOR_TOUCHING}s."
                    )

                    def _touch_job_periodically(
                        b_job_obj,
                        initial_ttr,
                        stop_event,
                        q_manager,
                        parent_logger,
                        job_ident,
                    ):
                        if not initial_ttr or initial_ttr < MIN_TTR_FOR_TOUCHING:
                            parent_logger.info(
                                f"Job {job_ident} TTR ({initial_ttr}s) too short or undefined, not starting toucher."
                            )
                            return

                        touch_interval = max(10, initial_ttr * TOUCH_INTERVAL_FACTOR)
                        parent_logger.info(
                            f"Starting toucher for job {job_ident} with TTR {initial_ttr}s, touch interval {touch_interval:.2f}s."
                        )

                        while not stop_event.is_set():
                            if stop_event.wait(timeout=touch_interval):
                                break
                            if stop_event.is_set():
                                break

                            try:
                                parent_logger.info(
                                    f"Toucher thread: Attempting to touch job {job_ident}."
                                )
                                q_manager.touch_job(b_job_obj)
                                parent_logger.info(
                                    f"Toucher thread: Successfully touched job {job_ident}."
                                )
                            except Exception as te:
                                parent_logger.error(
                                    f"Toucher thread: Failed to touch job {job_ident}: {te}"
                                )
                                break
                        parent_logger.info(
                            f"Toucher thread for job {job_ident} stopping."
                        )

                    if current_job_actual_ttr >= MIN_TTR_FOR_TOUCHING:
                        toucher_active_event.clear()
                        toucher_thread = threading.Thread(
                            target=_touch_job_periodically,
                            args=(
                                job_obj,
                                current_job_actual_ttr,
                                toucher_active_event,
                                self.queue_manager,
                                self.logger,
                                f"{job_id}/{crawl_id}",
                            ),
                        )
                        toucher_thread.daemon = True
                        toucher_thread.start()

                    success = False
                    try:
                        self.mongodb_client.update_one(
                            MONGO_CRAWL_JOB_COLLECTION,
                            {"crawl_id": crawl_id},
                            {
                                "$set": {
                                    "crawl_status": "crawling",
                                    "job_id": job_id,
                                    "updated_at": datetime.utcnow(),
                                }
                            },
                            upsert=True,
                        )
                        self.logger.info(
                            f"Set crawl_status to 'crawling' for crawl_id {crawl_id}."
                        )
                        success = self.crawl_job_processor.process_job(job_id, job_data)

                        if success:
                            self.logger.info(
                                f"Job {job_id} (crawl_id={crawl_id}) processed successfully by CrawlJobProcessor. Completing job."
                            )
                            self.queue_manager.complete_job(job_obj, job_data)
                        else:
                            job_stats_on_failure = self.queue_manager.get_job_stats(
                                job_obj
                            )
                            releases_count = (
                                job_stats_on_failure.get("releases", 0)
                                if job_stats_on_failure
                                else 0
                            )
                            self.logger.warning(
                                f"CrawlJobProcessor indicated failure for job {job_id} (crawl_id={crawl_id}). Current releases: {releases_count}."
                            )

                            if releases_count < self.max_retries:
                                self.logger.info(
                                    f"Retrying job {job_id} (crawl_id={crawl_id}) via Beanstalkd with 60s delay. Attempt {releases_count + 1}/{self.max_retries + 1}."
                                )
                                self.queue_manager.retry_job(
                                    job_obj, job_data, delay=60
                                )
                            else:
                                self.logger.error(
                                    f"Job {job_id} (crawl_id={crawl_id}) failed after {releases_count} releases (max {self.max_retries} allowed). Burying job."
                                )
                                self.queue_manager.bury_job(job_obj, job_data)

                    except Exception as processing_exception:
                        self.logger.error(
                            f"EXCEPTION during job processing for job {job_id} (crawl_id={crawl_id}): {processing_exception}"
                        )
                        try:
                            self.mongodb_client.update_one(
                                MONGO_CRAWL_JOB_COLLECTION,
                                {"crawl_id": crawl_id},
                                {
                                    "$set": {
                                        "crawl_status": "failed_exception",
                                        "error_message": str(processing_exception),
                                        "updated_at": datetime.utcnow(),
                                    }
                                },
                            )
                        except Exception as mongo_e:
                            self.logger.error(
                                f"Additionally failed to update MongoDB status to 'failed_exception' for {crawl_id}: {mongo_e}"
                            )

                        if job_obj and job_data:
                            job_stats_on_exception = self.queue_manager.get_job_stats(
                                job_obj
                            )
                            releases_count = (
                                job_stats_on_exception.get("releases", 0)
                                if job_stats_on_exception
                                else 0
                            )
                            if releases_count < self.max_retries:
                                self.logger.info(
                                    f"Retrying job {job_id} (crawl_id={crawl_id}) due to exception, with 60s delay. Attempt {releases_count + 1}/{self.max_retries + 1}."
                                )
                                self.queue_manager.retry_job(
                                    job_obj, job_data, delay=60
                                )
                            else:
                                self.logger.error(
                                    f"Job {job_id} (crawl_id={crawl_id}) failed due to exception after {releases_count} releases. Burying job."
                                )
                                self.queue_manager.bury_job(job_obj, job_data)
                        else:
                            self.logger.error(
                                f"Cannot retry/bury job {job_id} (crawl_id={crawl_id}) due to missing job_obj or job_data after exception."
                            )
                    finally:
                        if toucher_thread and toucher_thread.is_alive():
                            self.logger.info(
                                f"Signaling toucher thread for job {job_id}/{crawl_id} to stop."
                            )
                            toucher_active_event.set()
                            toucher_thread.join(
                                timeout=max(
                                    2.0,
                                    current_job_actual_ttr
                                    * TOUCH_INTERVAL_FACTOR
                                    * 0.5,
                                )
                            )
                            if toucher_thread.is_alive():
                                self.logger.warning(
                                    f"Toucher thread for job {job_id}/{crawl_id} did not stop in time."
                                )

                elif job_id is None and job_data is None and job_obj is None:
                    self.logger.debug("Job dequeue timeout. No job received.")
                else:
                    self.logger.warning(
                        f"Job unusual return from dequeue_job: job_id={job_id}, job_data is None: {job_data is None}, job_obj is None: {job_obj is None}"
                    )

            except Exception as e:
                self.logger.error(
                    f"UNHANDLED EXCEPTION in main processing loop (job_id: {job_id}, crawl_id: {crawl_id}): {e}",
                    exc_info=True,
                )
                if toucher_thread and toucher_thread.is_alive():
                    toucher_active_event.set()
                    toucher_thread.join(timeout=5.0)
                time.sleep(5)

        self.logger.info(f"CrawlJobListener_{self.instance_id} shutting down.")
        self.cleanup()

    def cleanup(self):
        self.logger.info("Cleaning up resources")
        if self.queue_manager and hasattr(self.queue_manager, "close"):
            self.queue_manager.close()
        if self.mongodb_client:
            try:
                self.mongodb_client.close()
            except Exception:
                pass


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Single-process crawler queue listener"
    )
    parser.add_argument("--instance-id", type=int, default=0, help="Worker instance ID")

    args = parser.parse_args()
    worker = CrawlJobListener(
        instance_id=args.instance_id,
    )
    try:
        worker.start()
    except KeyboardInterrupt:
        worker.shutdown_requested = True
    finally:
        worker.cleanup()


if __name__ == "__main__":
    main()
