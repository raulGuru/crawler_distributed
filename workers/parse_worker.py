import os
import sys
import logging
import time
import signal
import json
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.storage.file_storage import FileStorage
from config.base_settings import QUEUE_HOST, QUEUE_PORT, DB_URI, LOG_DIR


class ParseWorker:
    """
    Worker that processes HTML files from the parse queue
    """

    def __init__(self, queue_host=QUEUE_HOST, queue_port=QUEUE_PORT, db_uri=DB_URI):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.db_uri = db_uri
        self.running = False
        self.shutdown_requested = False

        # Set up logging
        self.logger = self._setup_logging()

        # Initialize components
        self.queue_manager = None
        self.mongodb_client = None
        self.file_storage = None

        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_logging(self):
        """Set up logging"""
        logger = logging.getLogger('ParseWorker')
        logger.setLevel(logging.INFO)

        # Create log directory if it doesn't exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'parse_worker.log'))
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def _handle_signal(self, signum, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.shutdown_requested = True

    def _initialize_components(self):
        """Initialize queue manager and database components"""
        try:
            self.logger.info("Initializing components")

            # Initialize queue manager
            self.queue_manager = QueueManager(host=self.queue_host, port=self.queue_port)

            # Initialize MongoDB client
            self.mongodb_client = MongoDBClient(uri=self.db_uri)

            # Initialize file storage
            self.file_storage = FileStorage(create_dirs=True)

            self.logger.info("Components initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False

    def _process_job(self, job_id, job_data):
        """Process a parse job"""
        self.logger.info(f"Processing job {job_id}")

        try:
            # Extract job parameters
            job_type = job_data.get('job_type')

            if job_type != 'parse':
                self.logger.error(f"Unsupported job type: {job_type}")
                return False

            url = job_data.get('url')
            html_file_path = job_data.get('html_file_path')
            crawl_id = job_data.get('crawl_id')

            if not (url and html_file_path):
                self.logger.error(f"Missing required parameters in job {job_id}")
                return False

            # Read HTML content
            html_content = self.file_storage.read_html(html_file_path)

            if not html_content:
                self.logger.error(f"Failed to read HTML content from {html_file_path}")
                return False

            # Process HTML content (simple demonstration)
            result = self._parse_html(html_content, url)

            # Store parsed result
            if result:
                self._store_parsed_result(result, url, crawl_id)

            return True

        except Exception as e:
            self.logger.error(f"Error processing job {job_id}: {str(e)}")
            return False

    def _parse_html(self, html_content, url):
        """
        Parse HTML content (simplified example)
        In a real implementation, this would use a more sophisticated parser
        """
        try:
            # Simple parsing for demonstration
            import re
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract basic metadata
            result = {
                'url': url,
                'title': soup.title.string if soup.title else None,
                'meta_description': None,
                'h1_headers': [],
                'links': [],
                'processed_at': datetime.utcnow().isoformat()
            }

            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and 'content' in meta_desc.attrs:
                result['meta_description'] = meta_desc['content']

            # Extract h1 headers
            for h1 in soup.find_all('h1'):
                if h1.string:
                    result['h1_headers'].append(h1.string.strip())

            # Extract links (limit to 100)
            for a in soup.find_all('a', href=True)[:100]:
                href = a['href']
                text = a.string if a.string else ''
                result['links'].append({
                    'href': href,
                    'text': text.strip() if text else ''
                })

            return result

        except Exception as e:
            self.logger.error(f"Error parsing HTML content: {str(e)}")
            return None

    def _store_parsed_result(self, result, url, crawl_id):
        """Store parsed result in MongoDB"""
        try:
            # Add storage metadata
            result['crawl_id'] = crawl_id
            result['stored_at'] = datetime.utcnow()

            # Store in MongoDB
            self.mongodb_client.insert_one('parsed_pages', result)
            self.logger.info(f"Stored parsed result for {url}")

            return True

        except Exception as e:
            self.logger.error(f"Error storing parsed result: {str(e)}")
            return False

    def start(self):
        """Start listening for jobs"""
        self.logger.info("Starting parse worker")

        # Initialize components
        if not self._initialize_components():
            self.logger.error("Failed to initialize components, exiting")
            return

        self.running = True

        # Main loop
        while self.running and not self.shutdown_requested:
            try:
                # Reserve a job
                self.logger.debug("Waiting for jobs")
                job_id, job_data = self.queue_manager.dequeue_job('parse_jobs', timeout=5)

                if job_id:
                    self.logger.info(f"Reserved job {job_id}")

                    # Process the job
                    success = self._process_job(job_id, job_data)

                    if success:
                        # Complete the job
                        self.queue_manager.complete_job(job_data)
                        self.logger.info(f"Job {job_id} processed successfully")
                    else:
                        # Fail the job
                        self.queue_manager.fail_job(job_data)
                        self.logger.error(f"Job {job_id} processing failed")

            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")

                # Sleep to avoid tight loop in case of persistent errors
                time.sleep(1)

            # Check for shutdown request
            if self.shutdown_requested:
                self.logger.info("Shutdown requested, stopping worker")
                break

        # Clean up
        self._cleanup()

    def stop(self):
        """Stop listening for jobs"""
        self.logger.info("Stopping parse worker")
        self.running = False
        self.shutdown_requested = True

    def _cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up resources")

        if self.queue_manager:
            self.queue_manager.close()

        if self.mongodb_client:
            self.mongodb_client.close()

        self.logger.info("Resources cleaned up")


if __name__ == '__main__':
    worker = ParseWorker()

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.logger.info("Keyboard interrupt received, stopping worker")
        worker.stop()
    except Exception as e:
        worker.logger.error(f"Unhandled exception: {str(e)}")
    finally:
        worker.logger.info("Parse worker stopped")