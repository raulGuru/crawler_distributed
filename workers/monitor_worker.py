import os
import sys
import logging
import time
import signal
import json
from datetime import datetime, timedelta

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR


class MonitorWorker:
    """
    Worker that monitors the system health and detects stalled jobs
    """

    def __init__(self, queue_host=QUEUE_HOST, queue_port=QUEUE_PORT):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.running = False
        self.shutdown_requested = False

        # Set up logging
        self.logger = self._setup_logging()

        # Initialize components
        self.queue_manager = None

        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_logging(self):
        """Set up logging"""
        logger = logging.getLogger('MonitorWorker')
        logger.setLevel(logging.INFO)

        # Create log directory if it doesn't exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'monitor_worker.log'))
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
        """Initialize queue manager component only"""
        try:
            self.logger.info("Initializing components")
            self.queue_manager = QueueManager(host=self.queue_host, port=self.queue_port)
            self.logger.info("Components initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False

    def _check_queue_health(self):
        """Check queue health"""
        try:
            self.logger.info("Checking queue health")

            queue_stats = self.queue_manager.get_stats()

            # Log queue statistics
            self.logger.info(f"Queue stats: {json.dumps(queue_stats)}")

            # Check for queue backlog
            for tube, stats in queue_stats.get('tubes', {}).items():
                ready_jobs = stats.get('ready', 0)
                buried_jobs = stats.get('buried', 0)

                if ready_jobs > 100:
                    self.logger.warning(f"Tube {tube} has {ready_jobs} ready jobs, potential backlog")

                if buried_jobs > 0:
                    self.logger.warning(f"Tube {tube} has {buried_jobs} buried jobs, may need attention")

            return True

        except Exception as e:
            self.logger.error(f"Error checking queue health: {str(e)}")
            return False

    def _check_stalled_jobs(self):
        """Check for stalled jobs (Beanstalkd only)"""
        try:
            self.logger.info("Checking for stalled jobs (Beanstalkd only)")
            # Example: log number of ready/reserved jobs
            stats = self.queue_manager.get_stats()
            self.logger.info(f"Queue stats: {stats}")
            # If you want to implement more advanced logic, you may need to extend QueueManager
            return True
        except Exception as e:
            self.logger.error(f"Error checking stalled jobs: {str(e)}")
            return False

    def _collect_system_metrics(self):
        """Collect system and queue metrics only (no MongoDB)"""
        try:
            self.logger.info("Collecting system and queue metrics")
            import psutil
            metrics = {
                'timestamp': datetime.utcnow().isoformat(),
                'system': {
                    'cpu_percent': psutil.cpu_percent(),
                    'memory_percent': psutil.virtual_memory().percent,
                    'disk_percent': psutil.disk_usage('/').percent
                },
                'queues': self.queue_manager.get_stats()
            }
            self.logger.info(f"Metrics: {json.dumps(metrics)}")
            return metrics
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {str(e)}")
            return None

    def start(self):
        """Start monitoring"""
        self.logger.info("Starting monitor worker")

        # Initialize components
        if not self._initialize_components():
            self.logger.error("Failed to initialize components, exiting")
            return

        self.running = True

        # Main loop
        while self.running and not self.shutdown_requested:
            try:
                # Check queue health
                self._check_queue_health()

                # Check for stalled jobs
                self._check_stalled_jobs()

                # Collect system metrics
                self._collect_system_metrics()

                # Sleep for monitoring interval
                self.logger.info("Sleeping for 60 seconds...")
                for _ in range(60):
                    if self.shutdown_requested:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")

                # Sleep to avoid tight loop in case of persistent errors
                time.sleep(10)

            # Check for shutdown request
            if self.shutdown_requested:
                self.logger.info("Shutdown requested, stopping worker")
                break

        # Clean up
        self._cleanup()

    def stop(self):
        """Stop monitoring"""
        self.logger.info("Stopping monitor worker")
        self.running = False
        self.shutdown_requested = True

    def _cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up resources")
        if self.queue_manager:
            self.queue_manager.close()
        self.logger.info("Resources cleaned up")


if __name__ == '__main__':
    worker = MonitorWorker()

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.logger.info("Keyboard interrupt received, stopping worker")
        worker.stop()
    except Exception as e:
        worker.logger.error(f"Unhandled exception: {str(e)}")
    finally:
        worker.logger.info("Monitor worker stopped")