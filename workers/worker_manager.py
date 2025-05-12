import os
import sys
import subprocess
from datetime import datetime
from typing import Dict, List, Optional


class WorkerManager:
    """
    Manages worker processes: start, stop, restart, and monitor workers.
    """

    def __init__(
        self,
        workers: dict,
        logger,
        log_dir: str,
        queue_host: str,
        queue_port: int,
        db_uri: str,
    ):
        self.workers = workers
        self.logger = logger
        self.log_dir = log_dir
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.db_uri = db_uri
        self.worker_processes: Dict[str, List[subprocess.Popen]] = {}
        self.worker_status: Dict[str, dict] = {}

    def start_worker(
        self, worker_name: str, instance_id: int = 0
    ) -> Optional[subprocess.Popen]:
        """Start a single worker process."""
        worker_config = self.workers[worker_name]
        script_path = worker_config["script"]
        cmd = [sys.executable, script_path]
        if worker_config["instances"] > 1:
            cmd.extend(["--instance-id", str(instance_id)])
        cmd.extend(worker_config.get("args", []))
        # cmd.extend(
        #     ["--queue-host", self.queue_host, "--queue-port", str(self.queue_port)]
        # )
        # if worker_name not in ['crawl_job_listener']:
        #     cmd.extend(['--db-uri', self.db_uri])
        log_file_path = os.path.join(self.log_dir, f"{worker_name}_{instance_id}.log")
        process = None
        with open(log_file_path, "a") as stdout_target:
            self.logger.info(
                f"Starting {worker_name} (instance {instance_id}): {' '.join(cmd)}"
            )
            process = subprocess.Popen(
                cmd,
                stdout=stdout_target,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            self.logger.info(
                f"Started {worker_name} (instance {instance_id}) with PID {process.pid}"
            )
        self.update_worker_status(worker_name, "running")
        return process

    def update_worker_status(
        self, worker_name: str, status: str, message: str = None
    ) -> None:
        """Update the status of a worker."""
        self.worker_status[worker_name] = {
            "status": status,
            "last_check": datetime.utcnow(),
            "message": message,
        }

    def start_all_workers(self) -> bool:
        """Start all configured workers."""
        self.logger.info("Starting all workers")
        for worker_name, config in self.workers.items():
            self.worker_processes[worker_name] = []
            for instance_id in range(config["instances"]):
                process = self.start_worker(worker_name, instance_id)
                if process:
                    self.worker_processes[worker_name].append(process)
                elif config["required"]:
                    self.logger.error(f"Failed to start required worker: {worker_name}")
                    return False
        for worker_name, config in self.workers.items():
            if config["required"] and (
                worker_name not in self.worker_processes
                or not self.worker_processes[worker_name]
            ):
                self.logger.error(f"Required worker not running: {worker_name}")
                return False
        self.logger.info("All workers started successfully")
        return True

    def check_worker_health(self) -> bool:
        """Check health of all worker processes and restart if needed."""
        self.logger.info("Checking worker health")
        restart_needed = []
        for worker_name, processes in self.worker_processes.items():
            config = self.workers[worker_name]
            expected_count = config["instances"]
            actual_count = len([p for p in processes if p.poll() is None])
            if actual_count < expected_count:
                self.logger.warning(
                    f"Worker {worker_name} has {actual_count}/{expected_count} instances running"
                )
                for i, process in enumerate(processes):
                    if process.poll() is not None:
                        exit_code = process.poll()
                        self.logger.warning(
                            f"Worker {worker_name} (instance {i}) terminated with exit code {exit_code}"
                        )
                        if config["restart"]:
                            restart_needed.append((worker_name, i, process))
            else:
                self.update_worker_status(worker_name, "running")
        for worker_name, instance_id, old_process in restart_needed:
            self.worker_processes[worker_name].remove(old_process)
            self.logger.info(f"Restarting {worker_name} (instance {instance_id})")
            new_process = self.start_worker(worker_name, instance_id)
            if new_process:
                self.worker_processes[worker_name].append(new_process)
        return True

    def shutdown_worker(
        self, worker_name: str, process: subprocess.Popen, timeout: int = 30
    ) -> bool:
        """Shutdown a worker process."""
        try:
            if process.poll() is not None:
                self.logger.info(
                    f"Worker {worker_name} (PID {process.pid}) already terminated"
                )
                return True
            self.logger.info(f"Sending SIGTERM to {worker_name} (PID {process.pid})")
            process.terminate()
            import time

            start_time = time.time()
            while process.poll() is None and (time.time() - start_time) < timeout:
                time.sleep(0.5)
            if process.poll() is None:
                self.logger.warning(
                    f"Worker {worker_name} (PID {process.pid}) did not terminate gracefully, sending SIGKILL"
                )
                process.kill()
                process.wait(timeout=5)
                return False
            self.logger.info(
                f"Worker {worker_name} (PID {process.pid}) terminated gracefully"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"Error shutting down worker {worker_name} (PID {process.pid}): {str(e)}"
            )
            return False

    def shutdown_all_workers(self) -> None:
        """Shutdown all worker processes."""
        self.logger.info("Shutting down all workers")
        for worker_name, processes in self.worker_processes.items():
            for process in processes:
                self.shutdown_worker(worker_name, process)
        self.worker_processes.clear()
