import os
import sys
import logging
import time
import signal
import subprocess
import threading
import argparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.utils.health_check import HealthCheck
from lib.utils.logging_utils import LoggingUtils
from workers.worker_manager import WorkerManager
from config.base_settings import (
    QUEUE_HOST, QUEUE_PORT, MONGO_URI, LOG_DIR,
    PROJECT_ROOT,
    CORE_WORKERS
)
from config.parser_settings import (
    ALL_PARSER_TASK_TYPES
)


class IntegrationService:
    """
    Service that coordinates all workers and handles their lifecycle
    """


    def __init__(self, queue_host: str = QUEUE_HOST, queue_port: int = QUEUE_PORT, mongo_uri: str = MONGO_URI,
                 health_check_interval: int = None) -> None:
        """
        Initialize the integration service

        Args:
            queue_host (str): Beanstalkd host
            queue_port (int): Beanstalkd port
            mongo_uri (str): MongoDB URI
            health_check_interval (int): Health check interval in seconds
        """
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.mongo_uri = mongo_uri
        self.health_check_interval = health_check_interval
        self.logger = self._setup_logging()
        self.mongodb_client = None
        self.queue_manager = None
        self.health_check = None
        self.running = False
        self.shutdown_requested = False
        self.health_check_thread = None

        self.workers = self._generate_worker_configurations()

        self.worker_manager = WorkerManager(
            workers=self.workers,
            logger=self.logger,
            log_dir=LOG_DIR,
            queue_host=self.queue_host,
            queue_port=self.queue_port,
            db_uri=self.mongo_uri
        )
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

    def _setup_logging(self):
        log_file = LoggingUtils.integration_service_log_path()
        logger = LoggingUtils.setup_logger(
            name="integration_service",
            log_file=log_file,
            level=None,
            console=True,
            json_format=False,
        )
        logger.propagate = False
        return logger

    def _generate_worker_configurations(self) -> dict:
        """Generates the full worker configuration dictionary including parser workers."""
        combined_workers = CORE_WORKERS.copy()
        self.logger.info(f"Found {len(ALL_PARSER_TASK_TYPES)} parser task types to configure.")

        for task_type, task_details in ALL_PARSER_TASK_TYPES.items():
            script_filename = task_details.get('worker_script_file')
            if not script_filename:
                self.logger.error(f"Missing 'worker_script_file' for task_type '{task_type}'. Skipping this worker.")
                continue

            script_path = os.path.join(PROJECT_ROOT, 'parser', 'workers', script_filename)
            num_instances = task_details.get('instances', 2)

            if task_type in combined_workers:
                self.logger.warning(f"Parser task_type '{task_type}' conflicts with a core worker name. Parser worker config will overwrite.")

            combined_workers[task_type] = {
                'script': script_path,
                'required': True,
                'instances': num_instances,
                'restart': True,
                'args': []
            }
            self.logger.info(f"Configured parser worker for '{task_type}': script='{script_path}', instances={num_instances}")

        self.logger.info(f"Total workers configured: {len(combined_workers)}")
        return combined_workers

    def _handle_signal(self, signum: int, frame: object) -> None:
        """Handle termination signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.shutdown_requested = True

    def _initialize_components(self) -> bool:
        """Initialize all components required for operation"""
        try:
            self.logger.info("Initializing components")

            # Initialize MongoDB client
            self.mongodb_client = MongoDBClient(logger=self.logger)

            # Initialize queue manager
            self.queue_manager = QueueManager(host=self.queue_host, port=self.queue_port, logger=self.logger)

            # Initialize health check
            self.health_check = HealthCheck(logger=self.logger)

            self.logger.info("Components initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            LoggingUtils.log_exception(self.logger, e, "Component initialization failed")
            return False

    def _verify_dependencies(self) -> bool:
        """Verify that all required dependencies are available"""
        self.logger.info("Verifying dependencies")

        # Check beanstalkd
        beanstalkd_check = self.health_check.check_beanstalkd(
            host=self.queue_host, port=self.queue_port
        )

        # Check MongoDB
        # mongodb_check = self.health_check.check_mongodb()

        # Check system resources
        system_check = self.health_check.check_system()

        # Log results
        for check in [beanstalkd_check, system_check]:
            if check['healthy']:
                self.logger.info(f"{check['component']} check passed: {check['message']}")
            else:
                self.logger.error(f"{check['component']} check failed: {check['message']}")

        # Overall health
        dependencies_ok = beanstalkd_check['healthy']

        if not dependencies_ok:
            self.logger.error("Dependency verification failed, cannot continue")
        else:
            self.logger.info("All dependencies verified successfully")

        # If system resources are low, log a warning but continue
        if not system_check['healthy']:
            self.logger.warning(f"System resource warning: {system_check['message']}")

        return dependencies_ok

    def _start_all_workers(self) -> bool:
        """Start all configured workers using WorkerManager."""
        self.logger.info("Attempting to start all workers via WorkerManager...")
        result = self.worker_manager.start_all_workers()
        self.logger.info(f"WorkerManager.start_all_workers() returned: {result}")
        return result

    def _check_worker_health(self) -> bool:
        """Check health of all worker processes using WorkerManager."""
        self.logger.debug("Attempting to check worker health via WorkerManager...")
        result = self.worker_manager.check_worker_health()
        self.logger.debug(f"WorkerManager.check_worker_health() returned: {result}")
        return result

    def _shutdown_worker(self, worker_name: str, process: subprocess.Popen, timeout: int = 30) -> bool:
        """Shutdown a worker process using WorkerManager."""
        return self.worker_manager.shutdown_worker(worker_name, process, timeout)

    def _shutdown_all_workers(self) -> None:
        """Shutdown all worker processes using WorkerManager."""
        self.logger.info("Attempting to shutdown all workers via WorkerManager...")
        self.worker_manager.shutdown_all_workers()
        self.logger.info("WorkerManager.shutdown_all_workers() completed.")

    def _run_health_check_thread(self) -> None:
        """Run periodic health checks in a separate thread"""
        self.logger.info("Health check thread started.")
        while self.running and not self.shutdown_requested:
            try:
                self.logger.debug("Health check thread: New cycle started.")
                # Run health check
                managed_worker_names = list(self.workers.keys())
                health_report = self.health_check.run_all_checks(worker_names_to_check=managed_worker_names)

                # Save health report
                self.health_check.save_health_report(health_report)
                self.logger.debug("Health check thread: Health report saved.")

                # Check worker health
                self.logger.info("Health check thread: Calling _check_worker_health().")
                self._check_worker_health()
                self.logger.info("Health check thread: _check_worker_health() completed.")

                # Wait for next check
                for _ in range(self.health_check_interval):
                    if self.shutdown_requested:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in health check thread: {str(e)}")
                LoggingUtils.log_exception(self.logger, e, "Health check error")
                time.sleep(10)  # Sleep briefly to avoid tight loop on persistent errors
        self.logger.info("Health check thread finished.")

    def _cleanup(self) -> None:
        """Cleanup resources"""
        self.logger.info("Cleaning up resources")

        if self.worker_manager:
            try:
                # WorkerManager.shutdown_all_workers() is called by self.stop()
                # No explicit close method in WorkerManager from snippet, assume it cleans up on its own
                pass
            except Exception as e:
                self.logger.error(f"Error during WorkerManager cleanup (if any): {str(e)}")

        # Close queue manager
        if self.queue_manager:
            try:
                self.queue_manager.close()
            except Exception as e:
                self.logger.error(f"Error closing queue manager: {str(e)}")

        # Close MongoDB client
        if self.mongodb_client:
            try:
                self.mongodb_client.close()
            except Exception as e:
                self.logger.error(f"Error closing MongoDB client: {str(e)}")

    def start(self) -> bool:
        """Start the integration service"""
        self.logger.info("Starting integration service")

        # Initialize components
        if not self._initialize_components():
            self.logger.error("Failed to initialize components, exiting")
            return False

        # Verify dependencies
        if not self._verify_dependencies():
            self.logger.error("Failed to verify dependencies, exiting")
            return False

        # Start all workers
        if not self._start_all_workers():
            self.logger.error("Failed to start all required workers, exiting")
            self._shutdown_all_workers()
            self._cleanup()
            return False

        self.running = True

        if self.health_check_interval is not None:
            # Start health check thread
            self.health_check_thread = threading.Thread(
                target=self._run_health_check_thread,
                daemon=True
            )
            self.health_check_thread.start()

        self.logger.info("Integration service started successfully")

        # Main loop - wait for shutdown signal
        try:
            self.logger.info("Integration service main loop started. Waiting for shutdown signal.")
            while self.running and not self.shutdown_requested:
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.shutdown_requested = True

        finally:
            # Shutdown
            self.stop()

        return True

    def stop(self) -> None:
        """Stop the integration service"""
        self.logger.info("Stopping integration service")

        self.running = False
        self.shutdown_requested = True

        # Wait for health check thread to terminate
        if self.health_check_thread and self.health_check_thread.is_alive():
            self.logger.info("Waiting for health check thread to terminate")
            self.health_check_thread.join(timeout=10)

        # Shutdown all workers
        self._shutdown_all_workers()

        # Cleanup resources
        self._cleanup()

        self.logger.info("Integration service stopped")

    def _start_worker(self, worker_name: str, instance_id: int = 0) -> subprocess.Popen:
        pass  # This method is now handled by WorkerManager

    def _update_worker_status(self, worker_name: str, status: str, message: str = None) -> None:
        pass  # This method is now handled by WorkerManager


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Integration service for distributed crawler')
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')
    parser.add_argument('--health-check-interval', type=int, default=None, help='Health check interval in seconds')

    args = parser.parse_args()

    try:
        service = IntegrationService(
            queue_host=args.queue_host,
            queue_port=args.queue_port,
            health_check_interval=args.health_check_interval
        )

        return 0 if service.start() else 1

    except Exception as e:
        logging.error(f"Unhandled exception in integration service: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())