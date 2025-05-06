import os
import sys
import logging
import time
import signal
import subprocess
import threading
import argparse
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.utils.logging_utils import LoggingUtils
from lib.utils.health_check import HealthCheck
from config.base_settings import (
    QUEUE_HOST, QUEUE_PORT, DB_URI, LOG_DIR,
    CRAWLER_QUEUE_LISTENER_PATH, MONITOR_WORKER_PATH, PARSE_WORKER_PATH
)
from workers.worker_manager import WorkerManager


class IntegrationService:
    """
    Service that coordinates all workers and handles their lifecycle
    """

    # Worker configurations
    WORKERS = {
        'crawler_queue_listener': {
            'script': CRAWLER_QUEUE_LISTENER_PATH,
            'required': True,  # System requires this worker
            'instances': 4,    # Number of instances to run
            'restart': True,   # Auto-restart if it crashes
            'args': []         # Additional command line arguments
        },
        'monitor_worker': {
            'script': MONITOR_WORKER_PATH,
            'required': True,
            'instances': 1,
            'restart': True,
            'args': []
        },
        # 'parse_worker': {
        #     'script': PARSE_WORKER_PATH,
        #     'required': True,
        #     'instances': 1,    # Multiple parser workers for parallel processing
        #     'restart': True,
        #     'args': []
        # }
    }

    def __init__(self, queue_host: str = QUEUE_HOST, queue_port: int = QUEUE_PORT, db_uri: str = DB_URI,
                 workers: dict = None, health_check_interval: int = 60) -> None:
        """
        Initialize the integration service

        Args:
            queue_host (str): Beanstalkd host
            queue_port (int): Beanstalkd port
            db_uri (str): MongoDB URI
            workers (dict, optional): Worker configuration, if None use defaults
            health_check_interval (int): Health check interval in seconds
        """
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.db_uri = db_uri
        self.workers = workers or self.WORKERS
        self.health_check_interval = health_check_interval
        self.logger = LoggingUtils.setup_logger('integration_service')
        self.mongodb_client = None
        self.queue_manager = None
        self.health_check = None
        self.running = False
        self.shutdown_requested = False
        self.health_check_thread = None
        self.worker_manager = WorkerManager(
            workers=self.workers,
            logger=self.logger,
            log_dir=LOG_DIR,
            queue_host=self.queue_host,
            queue_port=self.queue_port,
            db_uri=self.db_uri
        )
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

    def _handle_signal(self, signum: int, frame: object) -> None:
        """Handle termination signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.shutdown_requested = True

    def _initialize_components(self) -> bool:
        """Initialize all components required for operation"""
        try:
            self.logger.info("Initializing components")

            # Initialize MongoDB client
            self.mongodb_client = MongoDBClient(uri=self.db_uri)

            # Initialize queue manager
            self.queue_manager = QueueManager(host=self.queue_host, port=self.queue_port)

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
        mongodb_check = self.health_check.check_mongodb(uri=self.db_uri)

        # Check system resources
        system_check = self.health_check.check_system()

        # Log results
        for check in [beanstalkd_check, mongodb_check, system_check]:
            if check['healthy']:
                self.logger.info(f"{check['component']} check passed: {check['message']}")
            else:
                self.logger.error(f"{check['component']} check failed: {check['message']}")

        # Overall health
        dependencies_ok = beanstalkd_check['healthy'] and mongodb_check['healthy']

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
        return self.worker_manager.start_all_workers()

    def _check_worker_health(self) -> bool:
        """Check health of all worker processes using WorkerManager."""
        return self.worker_manager.check_worker_health()

    def _shutdown_worker(self, worker_name: str, process: subprocess.Popen, timeout: int = 30) -> bool:
        """Shutdown a worker process using WorkerManager."""
        return self.worker_manager.shutdown_worker(worker_name, process, timeout)

    def _shutdown_all_workers(self) -> None:
        """Shutdown all worker processes using WorkerManager."""
        self.worker_manager.shutdown_all_workers()

    def _run_health_check_thread(self) -> None:
        """Run periodic health checks in a separate thread"""
        while self.running and not self.shutdown_requested:
            try:
                # Run health check
                health_report = self.health_check.run_all_checks()

                # Save health report
                self.health_check.save_health_report(health_report)

                # Check worker health
                self._check_worker_health()

                # Wait for next check
                for _ in range(self.health_check_interval):
                    if self.shutdown_requested:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in health check thread: {str(e)}")
                LoggingUtils.log_exception(self.logger, e, "Health check error")
                time.sleep(10)  # Sleep briefly to avoid tight loop on persistent errors

    def _cleanup(self) -> None:
        """Cleanup resources"""
        self.logger.info("Cleaning up resources")

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

        # Start health check thread
        self.health_check_thread = threading.Thread(
            target=self._run_health_check_thread,
            daemon=True
        )
        self.health_check_thread.start()

        self.logger.info("Integration service started successfully")

        # Main loop - wait for shutdown signal
        try:
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
    parser.add_argument('--db-uri', default=DB_URI, help='MongoDB URI')
    parser.add_argument('--health-check-interval', type=int, default=60, help='Health check interval in seconds')

    args = parser.parse_args()

    try:
        service = IntegrationService(
            queue_host=args.queue_host,
            queue_port=args.queue_port,
            db_uri=args.db_uri,
            health_check_interval=args.health_check_interval
        )

        return 0 if service.start() else 1

    except Exception as e:
        logging.error(f"Unhandled exception in integration service: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())