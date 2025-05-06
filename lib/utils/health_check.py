import os
import sys
import socket
import time
import json
import psutil
import pymongo
import threading
import glob
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lib.utils.logging_utils import LoggingUtils
from lib.queue.beanstalkd_client import BeanstalkdClient
from config.base_settings import QUEUE_HOST, QUEUE_PORT, DB_URI, LOG_DIR


class HealthCheck:
    """
    Provides health check utilities for system components
    """

    def __init__(self, logger=None):
        """
        Initialize health check

        Args:
            logger (logging.Logger, optional): Logger to use
        """
        self.logger = logger or LoggingUtils.setup_logger("health_check")
        self.monitoring = False
        self.monitor_thread = None
        self.monitor_interval = 300  # Default interval of 5 minutes
        self.report_retention_days = 7  # Default retention period of 7 days

    def check_beanstalkd(self, host=QUEUE_HOST, port=QUEUE_PORT, timeout=5):
        """
        Check if Beanstalkd is accessible and operational

        Args:
            host (str): Beanstalkd host
            port (int): Beanstalkd port
            timeout (int): Connection timeout in seconds

        Returns:
            dict: Health check result
        """
        start_time = time.time()
        result = {
            'component': 'beanstalkd',
            'host': host,
            'port': port,
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': False,
            'status': 'unknown',
            'message': '',
            'metrics': {}
        }

        try:
            # First check if socket is reachable
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            sock.close()

            # Now check if beanstalkd responds to commands
            client = BeanstalkdClient(host=host, port=port)

            # Get server stats
            stats = client.stats()

            # Calculate response time
            response_time = time.time() - start_time

            # Extract key metrics
            result['metrics'] = {
                'current_jobs_ready': stats.get('current-jobs-ready', 0),
                'current_jobs_reserved': stats.get('current-jobs-reserved', 0),
                'current_jobs_buried': stats.get('current-jobs-buried', 0),
                'total_jobs': stats.get('total-jobs', 0),
                'current_connections': stats.get('current-connections', 0),
                'uptime': stats.get('uptime', 0),
                'response_time': response_time
            }

            result['healthy'] = True
            result['status'] = 'ok'
            result['message'] = f"Beanstalkd is running (uptime: {stats.get('uptime', 0)}s)"

            client.close()

        except socket.timeout:
            result['status'] = 'timeout'
            result['message'] = f"Connection to Beanstalkd timed out after {timeout}s"
            self.logger.error(f"Beanstalkd health check failed: {result['message']}")

        except socket.error as e:
            result['status'] = 'connection_error'
            result['message'] = f"Socket error connecting to Beanstalkd: {str(e)}"
            self.logger.error(f"Beanstalkd health check failed: {result['message']}")

        except Exception as e:
            result['status'] = 'error'
            result['message'] = f"Error checking Beanstalkd: {str(e)}"
            LoggingUtils.log_exception(self.logger, e, "Beanstalkd health check failed")

        return result

    def check_mongodb(self, uri=DB_URI, timeout=5000):
        """
        Check if MongoDB is accessible and operational

        Args:
            uri (str): MongoDB URI
            timeout (int): Connection timeout in ms

        Returns:
            dict: Health check result
        """
        start_time = time.time()
        result = {
            'component': 'mongodb',
            'uri': uri.split('@')[-1] if '@' in uri else uri,  # Hide credentials
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': False,
            'status': 'unknown',
            'message': '',
            'metrics': {}
        }

        try:
            # Connect to MongoDB with timeout
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=timeout)

            # Force connection
            server_info = client.server_info()

            # Run serverStatus command
            server_status = client.admin.command('serverStatus')

            # Calculate response time
            response_time = time.time() - start_time

            # Extract key metrics
            result['metrics'] = {
                'version': server_info.get('version', 'unknown'),
                'connections': server_status.get('connections', {}).get('current', 0),
                'active_connections': server_status.get('connections', {}).get('active', 0),
                'memory_mb': server_status.get('mem', {}).get('resident', 0),
                'uptime': server_status.get('uptime', 0),
                'response_time': response_time
            }

            result['healthy'] = True
            result['status'] = 'ok'
            result['message'] = f"MongoDB is running v{server_info.get('version', 'unknown')} (uptime: {server_status.get('uptime', 0)}s)"

            client.close()

        except pymongo.errors.ServerSelectionTimeoutError:
            result['status'] = 'timeout'
            result['message'] = f"MongoDB server selection timed out after {timeout}ms"
            self.logger.error(f"MongoDB health check failed: {result['message']}")

        except pymongo.errors.ConnectionFailure as e:
            result['status'] = 'connection_error'
            result['message'] = f"MongoDB connection failure: {str(e)}"
            self.logger.error(f"MongoDB health check failed: {result['message']}")

        except Exception as e:
            result['status'] = 'error'
            result['message'] = f"Error checking MongoDB: {str(e)}"
            LoggingUtils.log_exception(self.logger, e, "MongoDB health check failed")

        return result

    def check_system(self):
        """
        Check system resources (CPU, memory, disk)

        Returns:
            dict: Health check result
        """
        result = {
            'component': 'system',
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': True,
            'status': 'ok',
            'message': '',
            'metrics': {}
        }

        try:
            # CPU info
            cpu_percent = psutil.cpu_percent(interval=0.5)
            cpu_count = psutil.cpu_count()

            # Memory info
            memory = psutil.virtual_memory()

            # Disk info
            disk = psutil.disk_usage('/')

            # Process info
            process = psutil.Process()
            process_memory = process.memory_info()

            # Assemble metrics
            result['metrics'] = {
                'cpu': {
                    'percent': cpu_percent,
                    'count': cpu_count
                },
                'memory': {
                    'total_mb': memory.total / (1024 * 1024),
                    'available_mb': memory.available / (1024 * 1024),
                    'percent': memory.percent
                },
                'disk': {
                    'total_gb': disk.total / (1024 * 1024 * 1024),
                    'free_gb': disk.free / (1024 * 1024 * 1024),
                    'percent': disk.percent
                },
                'process': {
                    'rss_mb': process_memory.rss / (1024 * 1024),
                    'vms_mb': process_memory.vms / (1024 * 1024),
                    'cpu_percent': process.cpu_percent(interval=0.1)
                }
            }

            # Set warning levels
            warnings = []

            if cpu_percent > 90:
                warnings.append(f"High CPU usage: {cpu_percent}%")

            if memory.percent > 90:
                warnings.append(f"High memory usage: {memory.percent}%")

            if disk.percent > 90:
                warnings.append(f"High disk usage: {disk.percent}%")

            if warnings:
                result['status'] = 'warning'
                result['message'] = "; ".join(warnings)
                self.logger.warning(f"System health check warnings: {result['message']}")
            else:
                result['message'] = "System resources are within normal parameters"

        except Exception as e:
            result['healthy'] = False
            result['status'] = 'error'
            result['message'] = f"Error checking system resources: {str(e)}"
            LoggingUtils.log_exception(self.logger, e, "System health check failed")

        return result

    def check_component_processes(self, components=None):
        """
        Check if component processes are running

        Args:
            components (list, optional): List of component names to check

        Returns:
            dict: Health check result
        """
        if components is None:
            components = ['queue_listener', 'monitor_worker', 'parse_worker']

        result = {
            'component': 'processes',
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': True,
            'status': 'ok',
            'message': '',
            'metrics': {comp: {'running': False, 'pid': None, 'cpu_percent': None, 'memory_mb': None} for comp in components}
        }

        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.cmdline()
                    proc_name = proc.name()

                    # Check if this process matches any of our components
                    for component in components:
                        if any(component in cmd for cmd in cmdline) or component in proc_name:
                            # Get process info
                            p = psutil.Process(proc.pid)
                            result['metrics'][component] = {
                                'running': True,
                                'pid': proc.pid,
                                'cpu_percent': p.cpu_percent(interval=0.1),
                                'memory_mb': p.memory_info().rss / (1024 * 1024),
                                'started': datetime.fromtimestamp(p.create_time()).isoformat()
                            }
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass

            # Check for any missing components
            missing = [comp for comp in components if not result['metrics'][comp]['running']]

            if missing:
                result['healthy'] = False
                result['status'] = 'warning'
                result['message'] = f"Components not running: {', '.join(missing)}"
                self.logger.warning(f"Process health check: {result['message']}")
            else:
                result['message'] = f"All components are running: {', '.join(components)}"

        except Exception as e:
            result['healthy'] = False
            result['status'] = 'error'
            result['message'] = f"Error checking component processes: {str(e)}"
            LoggingUtils.log_exception(self.logger, e, "Process health check failed")

        return result

    def run_all_checks(self):
        """
        Run all health checks

        Returns:
            dict: All health check results
        """
        self.logger.info("Running all health checks")

        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'healthy': True,
            'checks': []
        }

        # Run all checks
        checks = [
            self.check_beanstalkd(),
            self.check_mongodb(),
            self.check_system(),
            self.check_component_processes()
        ]

        results['checks'] = checks

        # Overall health is False if any check is unhealthy
        results['healthy'] = all(check['healthy'] for check in checks)

        if not results['healthy']:
            self.logger.warning("Health check failed: Some components are unhealthy")
        else:
            self.logger.info("Health check passed: All components are healthy")

        return results

    def save_health_report(self, report=None):
        """
        Save health report to a file

        Args:
            report (dict, optional): Health report to save, if None run all checks

        Returns:
            str: Path to saved report
        """
        if report is None:
            report = self.run_all_checks()

        # Ensure log directory exists
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

        # Create health report filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(LOG_DIR, f"health_report_{timestamp}.json")

        # Save report as JSON
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        self.logger.info(f"Saved health report to {report_file}")
        return report_file

    def start_monitoring(self, interval=300, auto_cleanup=True, retention_days=7):
        """
        Start periodic health checks in the background

        Args:
            interval (int): Interval between checks in seconds
            auto_cleanup (bool): Automatically clean up old reports
            retention_days (int): Number of days to keep reports for

        Returns:
            bool: True if monitoring started, False if already running
        """
        if self.monitoring:
            self.logger.warning("Health check monitoring is already running")
            return False

        self.monitor_interval = interval
        self.report_retention_days = retention_days
        self.monitoring = True

        def monitor_task():
            self.logger.info(f"Starting health check monitoring (interval: {interval}s)")

            while self.monitoring:
                try:
                    # Run health check and save report
                    report = self.run_all_checks()
                    self.save_health_report(report)

                    # Clean up old reports if enabled
                    if auto_cleanup:
                        self.cleanup_old_reports(self.report_retention_days)

                    # Sleep for the specified interval
                    for _ in range(interval):
                        if not self.monitoring:
                            break
                        time.sleep(1)
                except Exception as e:
                    self.logger.error(f"Error in health check monitoring: {str(e)}")
                    LoggingUtils.log_exception(self.logger, e, "Health check monitoring error")
                    time.sleep(60)  # Sleep for a minute before retrying

            self.logger.info("Health check monitoring stopped")

        self.monitor_thread = threading.Thread(target=monitor_task, daemon=True)
        self.monitor_thread.start()

        return True

    def stop_monitoring(self):
        """
        Stop periodic health checks

        Returns:
            bool: True if monitoring was stopped, False if not running
        """
        if not self.monitoring:
            self.logger.warning("Health check monitoring is not running")
            return False

        self.logger.info("Stopping health check monitoring")
        self.monitoring = False

        # Wait for monitor thread to finish (with timeout)
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        self.monitor_thread = None
        return True

    def cleanup_old_reports(self, days=None):
        """
        Delete health reports older than specified days

        Args:
            days (int, optional): Number of days to keep reports for, defaults to self.report_retention_days

        Returns:
            int: Number of files deleted
        """
        if days is None:
            days = self.report_retention_days

        self.logger.info(f"Cleaning up health reports older than {days} days")

        # Calculate cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=days)

        # Get list of health report files
        report_pattern = os.path.join(LOG_DIR, "health_report_*.json")
        report_files = glob.glob(report_pattern)

        deleted_count = 0

        for file_path in report_files:
            try:
                # Get file modification time
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))

                # Delete if older than cutoff
                if file_time < cutoff_date:
                    os.remove(file_path)
                    deleted_count += 1
            except Exception as e:
                self.logger.error(f"Error cleaning up report file {file_path}: {str(e)}")

        if deleted_count > 0:
            self.logger.info(f"Deleted {deleted_count} old health report files")

        return deleted_count


if __name__ == '__main__':
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run health checks on system components')
    parser.add_argument('--monitor', action='store_true', help='Start continuous monitoring')
    parser.add_argument('--interval', type=int, default=300, help='Monitoring interval in seconds')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old health reports')
    parser.add_argument('--retention', type=int, default=7, help='Report retention period in days')
    args = parser.parse_args()

    # Initialize health check
    health_check = HealthCheck()

    # Clean up old reports if requested
    if args.cleanup:
        deleted = health_check.cleanup_old_reports(args.retention)
        print(f"Cleaned up {deleted} old health report files")

    # Run health check
    report = health_check.run_all_checks()
    report_file = health_check.save_health_report(report)

    # Print summary
    print(f"\nHealth Check Summary ({datetime.utcnow().isoformat()})")
    print(f"Overall: {'HEALTHY' if report['healthy'] else 'UNHEALTHY'}")
    print("-" * 50)

    for check in report['checks']:
        status_icon = "✅" if check['healthy'] else "❌"
        print(f"{status_icon} {check['component']}: {check['status']} - {check['message']}")

    print(f"\nFull report saved to: {report_file}")

    # Start monitoring if requested
    if args.monitor:
        print(f"\nStarting health check monitoring (interval: {args.interval}s)")
        print("Press Ctrl+C to stop monitoring")

        health_check.start_monitoring(interval=args.interval, retention_days=args.retention)

        try:
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping health check monitoring")
            health_check.stop_monitoring()