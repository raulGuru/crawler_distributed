import os
import sys
import signal
import time
import abc
import logging
import pymongo
from datetime import datetime
from bs4 import BeautifulSoup
from bson import ObjectId
from bson.errors import InvalidId


# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.utils.logging_utils import LoggingUtils
from lib.utils.extractor_base import BaseExtractor
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR


class RetryableError(Exception):
    """Custom exception for retryable errors in task processing."""
    pass


class NonRetryableError(Exception):
    """Custom exception for non-retryable errors in task processing."""
    pass


class BaseParserWorker(abc.ABC):
    """
    Base class for parser workers. Handles common functionalities:
    - Connecting to Beanstalkd and MongoDB.
    - Looping to dequeue jobs from a specific tube.
    - Deserializing job messages.
    - Calling an abstract process_task(job_data) method.
    - Managing job lifecycle (delete on success, release/bury on failure).
    - Basic error handling and logging.
    - Graceful shutdown.
    - Common extraction patterns.
    """

    def __init__(self, tube_name: str, task_type: str, instance_id: int = 0, parser_type: str = "html.parser"):
        """
        Initialize the BaseParserWorker.

        Args:
            tube_name (str): The Beanstalkd tube name this worker will listen to.
            task_type (str): A string identifying the type of task this worker handles.
            instance_id (int): The instance ID of this worker.
            parser_type (str): BeautifulSoup parser type (default: html.parser).
        """
        self.tube_name = tube_name
        self.task_type = task_type
        self.instance_id = instance_id
        self.parser_type = parser_type
        self.worker_name = self.__class__.__name__  # Store the concrete worker class name

        self._setup_logging()

        self.mongodb_client = MongoDBClient(logger=self.logger)
        self.queue_manager = QueueManager(host=QUEUE_HOST, port=QUEUE_PORT, logger=self.logger)
        # Initialize the base extractor
        self.extractor = BaseExtractor()

        self.running = False
        self.shutdown_requested = False
        self._setup_signal_handlers()

    def _handle_signal(self, signum: int, frame: object) -> None:
        """Handles termination signals."""
        self.logger.info(f"[{self.worker_name}] Received signal {signal.Signals(signum).name}. Initiating shutdown...")
        self.shutdown_requested = True
        self.running = False

    def _setup_signal_handlers(self) -> None:
        """Sets up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_logging(self):
        logger_name = f"parser_worker.{self.task_type}.{self.instance_id}"
        full_log_file_path = LoggingUtils.parser_worker_log_path(
            self.task_type, self.instance_id
        )

        self.logger = LoggingUtils.setup_logger(
            name=logger_name,
            log_file=full_log_file_path,
            level=None,
            console=False,
            json_format=False,
        )
        self.logger.propagate = False
        self.logger.info(f"[{self.worker_name}] Logging for {logger_name} (instance {self.instance_id}) initialized. Log file: {full_log_file_path}. Propagation set to False.")

    def _validate_job_data(self, job_id: str, job_data: dict) -> bool:
        """
        Validates the basic structure of job_data.
        Logs an error and returns False if validation fails.
        """
        required_keys = ['document_id', 'html_file_path', 'task_type', 'url', 'domain']
        for key in required_keys:
            if key not in job_data:
                self.logger.error(f"[{self.worker_name}] Missing critical key '{key}' in job data for job ID {job_id}. Invalid job.")
                return False
        if job_data['task_type'] != self.task_type:
            self.logger.error(f"[{self.worker_name}] Job task_type '{job_data['task_type']}' does not match worker task_type '{self.task_type}' for job ID {job_id}.")
            return False
        return True

    def _validate_html_file(self, html_path: str, doc_id_str: str) -> None:
        """Validate HTML file exists and is accessible."""
        if not os.path.exists(html_path):
            self.logger.error(f"[{self.worker_name}] HTML file not found: {html_path} for doc_id: {doc_id_str}")
            raise NonRetryableError(f"[{self.worker_name}] HTML file not found: {html_path}")

    def _read_html_file(self, html_path: str) -> str:
        """Read HTML content from file with proper error handling."""
        try:
            with open(html_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            raise NonRetryableError(f"[{self.worker_name}] HTML file disappeared: {html_path}")
        except PermissionError:
            raise NonRetryableError(f"[{self.worker_name}] Permission denied: {html_path}")
        except UnicodeDecodeError:
            raise RetryableError(f"[{self.worker_name}] Encoding error reading {html_path}")
        except Exception as e:
            raise RetryableError(f"[{self.worker_name}] Unexpected error reading {html_path}: {e}")

    def _create_soup(self, html_content: str) -> BeautifulSoup:
        """Create BeautifulSoup object with error handling."""
        try:
            return BeautifulSoup(html_content, self.parser_type)
        except Exception as e:
            self.logger.error(f"[{self.worker_name}] Failed to create BeautifulSoup object: {e}")
            raise NonRetryableError(f"[{self.worker_name}] HTML parsing initialization failed: {e}")

    def _store_in_mongodb(self, data: dict, doc_id_str: str, data_field_name: str) -> None:
        """Store extracted data in MongoDB with standard pattern."""
        try:
            mongo_document_id = ObjectId(doc_id_str)
        except InvalidId:
            self.logger.error(f"[{self.worker_name}] Invalid document_id format: {doc_id_str}. Cannot convert to ObjectId.")
            raise NonRetryableError(f"[{self.worker_name}] Invalid document_id format: {doc_id_str}")

        current_utc_time = datetime.utcnow()
        update_payload = {
            "$set": {
                data_field_name: data,
                f"worker_completion_timestamps.{self.task_type}": current_utc_time,
                "last_updated_at": current_utc_time,
            }
        }

        try:
            db_result = self.mongodb_client.update_one(
                collection_name="parsed_html_data",
                query={"_id": mongo_document_id},
                update=update_payload,
                upsert=True,
            )
            if db_result['modified_count'] == 0 and not db_result['upserted_id']:
                self.logger.warning(f"[{self.worker_name}] No document modified for {doc_id_str}")
        except pymongo.errors.DuplicateKeyError:
            raise NonRetryableError(f"[{self.worker_name}] Duplicate key error for {doc_id_str}")
        except pymongo.errors.NetworkTimeout:
            raise RetryableError(f"[{self.worker_name}] MongoDB timeout for {doc_id_str}")
        except Exception as e:
            self.logger.error(f"[{self.worker_name}] MongoDB update failed for doc_id {doc_id_str}: {e}")
            raise RetryableError(f"[{self.worker_name}] MongoDB update failed for doc_id {doc_id_str}: {e}")

    def process_task(self, job_data: dict) -> None:
        """Process a task with common workflow."""
        start_time = time.time()
        doc_id_str = job_data["document_id"]
        html_path = job_data["html_file_path"]
        url = job_data["url"]
        domain = job_data["domain"]

        self.logger.debug(f"[{self.worker_name}] Processing task for doc_id: {doc_id_str}, html_path: {html_path}")

        # Store job_data for access in worker methods
        self.job_data = job_data

        # Common validation
        self._validate_html_file(html_path, doc_id_str)

        # Read HTML content
        html_content = self._read_html_file(html_path)

        # Extract data (delegated to concrete implementation)
        extracted_data = self.extract_data(html_content, html_path, doc_id_str, url, domain)

        # Store in MongoDB (using common method)
        self._store_in_mongodb(extracted_data, doc_id_str, self.get_data_field_name())

        processing_time = time.time() - start_time
        self.logger.info(
            f"[{self.worker_name}] Successfully processed and updated {self.task_type} for doc_id: {doc_id_str} "
            f"in {processing_time:.2f}s"
        )

    @abc.abstractmethod
    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """
        Extract specific data - must be implemented by concrete workers.

        Args:
            html_content (str): The HTML content to extract data from.
            html_path (str): Path to the HTML file.
            doc_id_str (str): Document ID string.
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: The extracted data.
        """
        pass

    @abc.abstractmethod
    def get_data_field_name(self) -> str:
        """
        Return the MongoDB field name for this worker's data.

        Returns:
            str: The field name in MongoDB where this worker's data will be stored.
        """
        pass

    def run(self) -> None:
        """
        Main loop for the worker.
        Continuously dequeues jobs, processes them, and manages their lifecycle.
        """
        self.running = True
        self.logger.info(f"[{self.worker_name}] Worker for task type '{self.task_type}' (instance {self.instance_id}) started. Listening on tube '{self.tube_name}'.")
        self.queue_manager.watch_tube(self.tube_name)

        while self.running and not self.shutdown_requested:
            job_id, job_data, job_obj = None, None, None
            try:
                job_id, job_data, job_obj = self.queue_manager.dequeue_job(self.tube_name, timeout=5)

                if job_id is None:
                    time.sleep(0.5)
                    continue

                self.logger.info(f"[{self.worker_name}] Dequeued job ID: {job_id} from tube '{self.tube_name}'")

                if not self._validate_job_data(job_id, job_data):
                    self.logger.error(f"[{self.worker_name}] Invalid job data for job ID {job_id}. Burying job.")
                    self.queue_manager.fail_job(job_obj, job_data)
                    continue

                try:
                    self.process_task(job_data)
                    self.logger.info(f"[{self.worker_name}] Successfully processed job ID: {job_id} for task '{self.task_type}'.")
                    self.queue_manager.complete_job(job_obj, job_data)
                except RetryableError as e:
                    self.logger.warning(f"[{self.worker_name}] Retryable error processing job ID {job_id} for task '{self.task_type}': {e}. Releasing job.")
                    self.queue_manager.retry_job(job_obj, job_data)
                except NonRetryableError as e:
                    self.logger.error(f"[{self.worker_name}] Non-retryable error processing job ID {job_id} for task '{self.task_type}': {e}. Burying job.")
                    self.queue_manager.fail_job(job_obj, job_data)
                except Exception as e:
                    self.logger.error(f"[{self.worker_name}] Unexpected error processing job ID {job_id} for task '{self.task_type}': {e}")
                    LoggingUtils.log_exception(self.logger, e, f"[{self.worker_name}] Unexpected error in process_task for job ID {job_id}, task {self.task_type}")
                    self.queue_manager.fail_job(job_obj, job_data)

            except ConnectionError as e:
                self.logger.error(f"[{self.worker_name}] Connection error with Beanstalkd for task '{self.task_type}': {e}. Retrying connection in 10s.")
                time.sleep(10)
            except Exception as e:
                self.logger.critical(f"[{self.worker_name}] Critical error in worker main loop for task '{self.task_type}': {e}")
                LoggingUtils.log_exception(self.logger, e, f"[{self.worker_name}] Critical error in worker main loop, task {self.task_type}")
                if job_obj:
                    try:
                        self.logger.warning(f"[{self.worker_name}] Releasing job ID {job_id} due to critical loop error in task '{self.task_type}'.")
                        self.queue_manager.retry_job(job_obj, job_data, delay=60)
                    except Exception as release_e:
                        self.logger.error(f"[{self.worker_name}] Failed to release job ID {job_id} during critical error handling for task '{self.task_type}': {release_e}")
                time.sleep(5)

        self.logger.info(f"[{self.worker_name}] Shutdown initiated for worker task type '{self.task_type}' (instance {self.instance_id}).")
        self._cleanup()

    def _cleanup(self) -> None:
        """Cleans up resources like database and queue connections."""
        self.logger.info(f"[{self.worker_name}] Cleaning up resources for task type '{self.task_type}' (instance {self.instance_id})...")
        try:
            if self.queue_manager:
                self.queue_manager.close()
                self.logger.info(f"[{self.worker_name}] Beanstalkd connection closed for task '{self.task_type}' (instance {self.instance_id}).")
        except Exception as e:
            self.logger.error(f"[{self.worker_name}] Error closing QueueManager for task '{self.task_type}' (instance {self.instance_id}): {e}")
            LoggingUtils.log_exception(self.logger, e, f"[{self.worker_name}] Error closing QueueManager for task {self.task_type}")

        try:
            if self.mongodb_client:
                self.mongodb_client.close()
                self.logger.info(f"[{self.worker_name}] MongoDB connection closed for task '{self.task_type}' (instance {self.instance_id}).")
        except Exception as e:
            self.logger.error(f"[{self.worker_name}] Error closing MongoDBClient for task '{self.task_type}' (instance {self.instance_id}): {e}")
            LoggingUtils.log_exception(self.logger, e, f"[{self.worker_name}] Error closing MongoDBClient for task {self.task_type}")

        self.logger.info(f"[{self.worker_name}] Cleanup complete for task type '{self.task_type}' (instance {self.instance_id}). Worker stopped.")

    def start(self) -> None:
        """Public method to start the worker's main loop.
           Useful if the worker is instantiated and started by another script.
        """
        self.shutdown_requested = False
        self.run()

    def stop(self) -> None:
        """Public method to signal the worker to stop its loop and cleanup."""
        self.logger.info(f"[{self.worker_name}] Stop method called for task '{self.task_type}' (instance {self.instance_id}). Requesting shutdown.")
        self.shutdown_requested = True
        self.running = False