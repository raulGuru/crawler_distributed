import os
import time
import subprocess
from datetime import datetime
from typing import Any, Dict

class CrawlJobProcessor:
    """
    Handles the processing of crawl jobs: builds Scrapy command, runs the process,
    and logs results to MongoDB.
    """
    def __init__(self, logger, mongodb_client, scrapy_path: str, log_dir: str, mongo_collection: str):
        self.logger = logger
        self.mongodb_client = mongodb_client
        self.scrapy_path = scrapy_path
        self.log_dir = log_dir
        self.mongo_collection = mongo_collection

    def build_scrapy_command(self, job_id: Any, job_data: Dict[str, Any]) -> list:
        """
        Build the Scrapy command for the given job.
        """
        single_url_str = 'True' if job_data.get('single_url') else 'False'
        use_sitemap_str = 'True' if job_data.get('use_sitemap') else 'False'
        spider_name = 'url_spider' if job_data.get('single_url') else 'domain_spider'
        max_pages = int(job_data.get('max_pages', 50))
        cmd = [
            self.scrapy_path, 'crawl', spider_name,
            '-a', f"job_id={job_id}",
            '-a', f"max_pages={max_pages}",
            '-a', f"single_url={single_url_str}",
            '-a', f"use_sitemap={use_sitemap_str}"
        ]
        if job_data.get('domain'):
            cmd.extend(['-a', f"domain={job_data['domain']}"])
        if job_data.get('url'):
            cmd.extend(['-a', f"url={job_data['url']}"])
        if job_data.get('crawl_id'):
            cmd.extend(['-a', f"crawl_id={job_data['crawl_id']}"])
        exclude_keys = {'job_id', 'max_pages', 'single_url', 'use_sitemap', 'domain', 'url', 'crawl_id'}
        for key, value in job_data.items():
            if key not in exclude_keys and value is not None:
                cmd.extend(['-a', f"{key}={value}"])
        log_file = os.path.join(self.log_dir, f"scrapy_{job_data['crawl_id']}.log")
        cmd.extend(['--logfile', log_file])
        return cmd

    def log_job_to_mongo(self, job_id: Any, job_data: Dict[str, Any], status: str, duration: float, stdout: str, stderr: str) -> None:
        """
        Log the job result to MongoDB.
        """
        try:
            crawl_id = job_data.get('crawl_id')
            if crawl_id:
                update_data = {
                    'job_id': job_id,
                    'job_data': job_data,
                    'crawl_status': status,
                    'duration': duration,
                    'stdout': stdout,
                    'stderr': stderr,
                    'updated_at': datetime.utcnow(),
                }
                self.mongodb_client.update_one(
                    self.mongo_collection,
                    {'crawl_id': crawl_id},
                    {'$set': update_data}
                )
                self.logger.info(f"Updated crawl_jobs document for crawl_id {crawl_id} with crawl_status '{status}'")
            else:
                self.logger.error(f"No crawl_id found in job_data for job {job_id}, cannot update MongoDB.")
        except Exception as e:
            self.logger.error(f"Failed to update job {job_id} (crawl_id={job_data.get('crawl_id')}) in MongoDB: {str(e)}")

    def process_job(self, job_id: Any, job_data: Dict[str, Any]) -> bool:
        """
        Process a single crawl job: run Scrapy and log results.
        """
        self.logger.info(f"Processing job {job_id}")
        try:
            cmd = self.build_scrapy_command(job_id, job_data)
            self.logger.info(f"Running Scrapy command: {' '.join(cmd)}")
            start_time = time.time()
            process = subprocess.Popen(
                cmd,
                cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'spider_project')),
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()
            end_time = time.time()
            duration = end_time - start_time
            status = 'completed' if process.returncode == 0 else 'failed'
            self.logger.info(f"Scrapy process {status} for job {job_id} in {duration:.2f} seconds")
            if stdout:
                self.logger.info(f"Scrapy output: {stdout}")
            if stderr:
                self.logger.error(f"Scrapy errors: {stderr}")
            self.log_job_to_mongo(job_id, job_data, status, duration, stdout, stderr)
            return status == 'completed'
        except Exception as e:
            self.logger.error(f"Error running Scrapy for job {job_id}: {str(e)}")
            self.log_job_to_mongo(job_id, job_data, 'failed', 0, '', str(e))
            return False