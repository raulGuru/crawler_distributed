import os
import sys
import signal
import time
import json
import abc
import logging

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.utils.logging_utils import LoggingUtils
from config.base_settings import QUEUE_HOST, QUEUE_PORT, DB_URI, LOG_DIR


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
    """

    def __init__(self, tube_name: str, task_type: str, instance_id: int = 0):
        """
        Initialize the BaseParserWorker.

        Args:
            tube_name (str): The Beanstalkd tube name this worker will listen to.
            task_type (str): A string identifying the type of task this worker handles.
            instance_id (int): The instance ID of this worker.
        """
        self.tube_name = tube_name
        self.task_type = task_type
        self.instance_id = instance_id

        self._setup_logging()

        self.mongodb_client = MongoDBClient(uri=DB_URI)
        self.queue_manager = QueueManager(host=QUEUE_HOST, port=QUEUE_PORT)

        self.running = False
        self.shutdown_requested = False
        self._setup_signal_handlers()

    def _handle_signal(self, signum: int, frame: object) -> None:
        """Handles termination signals."""
        self.logger.info(f"Received signal {signal.Signals(signum).name}. Initiating shutdown...")
        self.shutdown_requested = True
        self.running = False

    def _setup_signal_handlers(self) -> None:
        """Sets up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    @abc.abstractmethod
    def process_task(self, job_data: dict) -> None:
        """
        Process the actual parsing task.
        This method must be implemented by concrete worker subclasses.

        Args:
            job_data (dict): The deserialized data from the Beanstalkd job.
                             Typically includes 'document_id', 'html_file_path', etc.

        Raises:
            RetryableError: If the task failed in a way that warrants a retry.
            NonRetryableError: If the task failed permanently.
            Any other Exception: For unexpected errors, which will be treated as non-retryable.
        """
        pass

    def _validate_job_data(self, job_id: str, job_data: dict) -> bool:
        """
        Validates the basic structure of job_data.
        Logs an error and returns False if validation fails.
        """
        required_keys = ['document_id', 'html_file_path', 'task_type']
        for key in required_keys:
            if key not in job_data:
                self.logger.error(f"Missing critical key '{key}' in job data for job ID {job_id}. Invalid job.")
                return False
        if job_data['task_type'] != self.task_type:
            self.logger.error(f"Job task_type '{job_data['task_type']}' does not match worker task_type '{self.task_type}' for job ID {job_id}.")
            return False
        return True

    def _setup_logging(self):
        logger_name = f"parser_worker.{self.task_type}.{self.instance_id}"
        sanitized_task_type_for_filename = self.task_type.replace('/', '_')
        log_file_name = f"{sanitized_task_type_for_filename}_{self.instance_id}.log"
        full_log_file_path = os.path.join(LOG_DIR, log_file_name)

        self.logger = LoggingUtils.setup_logger(
            name=logger_name,
            log_file=full_log_file_path,
            level=logging.INFO,
            console=False
        )
        self.logger.propagate = False
        self.logger.info(f"Logging for {logger_name} (instance {self.instance_id}) initialized. Log file: {full_log_file_path}. Propagation set to False.")

    def run(self) -> None:
        """
        Main loop for the worker.
        Continuously dequeues jobs, processes them, and manages their lifecycle.
        """
        self.running = True
        self.logger.info(f"Worker for task type '{self.task_type}' (instance {self.instance_id}) started. Listening on tube '{self.tube_name}'.")
        self.queue_manager.watch_tube(self.tube_name)

        while self.running and not self.shutdown_requested:
            job_id, job_data, job_obj = None, None, None
            try:
                job_id, job_data, job_obj = self.queue_manager.dequeue_job(self.tube_name, timeout=5)

                if job_id is None:
                    time.sleep(0.5)
                    continue

                self.logger.info(f"Dequeued job ID: {job_id} from tube '{self.tube_name}'")

                if not self._validate_job_data(job_id, job_data):
                    self.logger.error(f"Invalid job data for job ID {job_id}. Burying job.")
                    self.queue_manager.fail_job(job_obj, job_data)
                    continue

                try:
                    self.process_task(job_data)
                    self.logger.info(f"Successfully processed job ID: {job_id} for task '{self.task_type}'.")
                    self.queue_manager.complete_job(job_obj, job_data)
                except RetryableError as e:
                    self.logger.warning(f"Retryable error processing job ID {job_id} for task '{self.task_type}': {e}. Releasing job.")
                    self.queue_manager.retry_job(job_obj, job_data)
                except NonRetryableError as e:
                    self.logger.error(f"Non-retryable error processing job ID {job_id} for task '{self.task_type}': {e}. Burying job.")
                    self.queue_manager.fail_job(job_obj, job_data)
                except Exception as e:
                    self.logger.error(f"Unexpected error processing job ID {job_id} for task '{self.task_type}': {e}")
                    LoggingUtils.log_exception(self.logger, e, f"Unexpected error in process_task for job ID {job_id}, task {self.task_type}")
                    self.queue_manager.fail_job(job_obj, job_data)

            except ConnectionError as e:
                self.logger.error(f"Connection error with Beanstalkd for task '{self.task_type}': {e}. Retrying connection in 10s.")
                time.sleep(10)
            except Exception as e:
                self.logger.critical(f"Critical error in worker main loop for task '{self.task_type}': {e}")
                LoggingUtils.log_exception(self.logger, e, f"Critical error in worker main loop, task {self.task_type}")
                if job_obj:
                    try:
                        self.logger.warning(f"Releasing job ID {job_id} due to critical loop error in task '{self.task_type}'.")
                        self.queue_manager.retry_job(job_obj, job_data, delay=60)
                    except Exception as release_e:
                        self.logger.error(f"Failed to release job ID {job_id} during critical error handling for task '{self.task_type}': {release_e}")
                time.sleep(5)

        self.logger.info(f"Shutdown initiated for worker task type '{self.task_type}' (instance {self.instance_id}).")
        self._cleanup()

    def _cleanup(self) -> None:
        """Cleans up resources like database and queue connections."""
        self.logger.info(f"Cleaning up resources for task type '{self.task_type}' (instance {self.instance_id})...")
        try:
            if self.queue_manager:
                self.queue_manager.close()
                self.logger.info(f"Beanstalkd connection closed for task '{self.task_type}' (instance {self.instance_id}).")
        except Exception as e:
            self.logger.error(f"Error closing QueueManager for task '{self.task_type}' (instance {self.instance_id}): {e}")
            LoggingUtils.log_exception(self.logger, e, f"Error closing QueueManager for task {self.task_type}")

        try:
            if self.mongodb_client:
                self.mongodb_client.close()
                self.logger.info(f"MongoDB connection closed for task '{self.task_type}' (instance {self.instance_id}).")
        except Exception as e:
            self.logger.error(f"Error closing MongoDBClient for task '{self.task_type}' (instance {self.instance_id}): {e}")
            LoggingUtils.log_exception(self.logger, e, f"Error closing MongoDBClient for task {self.task_type}")

        self.logger.info(f"Cleanup complete for task type '{self.task_type}' (instance {self.instance_id}). Worker stopped.")

    def start(self) -> None:
        """Public method to start the worker's main loop.
           Useful if the worker is instantiated and started by another script.
        """
        self.shutdown_requested = False
        self.run()

    def stop(self) -> None:
        """Public method to signal the worker to stop its loop and cleanup."""
        self.logger.info(f"Stop method called for task '{self.task_type}' (instance {self.instance_id}). Requesting shutdown.")
        self.shutdown_requested = True
        self.running = False

    def _initialize_components(self):
        """Initialize queue manager and MongoDB client for the worker instance."""
        # ... existing code ...