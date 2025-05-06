import logging
import time
import uuid
from datetime import datetime, timedelta
from pymongo import ReturnDocument, ASCENDING, DESCENDING
from ..utils.log_formatter import format_log_message, get_job_specific_logger, log_exception
from ..utils.env_manager import get_job_params
from config.base_settings import LOG_DIR


class StateManager:
    """
    Manages crawler state in MongoDB
    """

    # Default collection names
    CRAWL_JOBS_COLLECTION = 'crawl_jobs'
    CRAWL_STATS_COLLECTION = 'crawl_stats'

    def __init__(self, mongodb_client):
        self.mongodb_client = mongodb_client
        self.logger = logging.getLogger(self.__class__.__name__)

        # Create indexes
        self._create_indexes()

    def _create_indexes(self):
        """Create necessary indexes"""
        try:
            # Check if mongodb_client is not None before creating indexes
            if self.mongodb_client is not None:
                # Crawl jobs indexes
                self.mongodb_client.create_index(
                    self.CRAWL_JOBS_COLLECTION,
                    [('crawl_id', ASCENDING)],
                    unique=True
                )
                self.mongodb_client.create_index(
                    self.CRAWL_JOBS_COLLECTION,
                    [('status', ASCENDING), ('created_at', ASCENDING)]
                )

                # Crawl stats indexes
                self.mongodb_client.create_index(
                    self.CRAWL_STATS_COLLECTION,
                    [('crawl_id', ASCENDING)],
                    unique=True
                )

                self.logger.info("Created indexes for state collections")
            else:
                self.logger.error("MongoDB client is None, cannot create indexes")
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {str(e)}")

    def check_job_exists(self, domain=None, url=None, timeframe_hours=24):
        """
        Check if a job for the given domain or URL already exists
        and was created within the specified timeframe

        Args:
            domain (str, optional): Domain to check
            url (str, optional): URL to check
            timeframe_hours (int): Time frame in hours to check for existing jobs

        Returns:
            dict or None: The existing job if found, None otherwise
        """
        if not domain and not url:
            return None

        try:
            # Calculate the cutoff time
            cutoff_time = datetime.utcnow() - timedelta(hours=timeframe_hours)

            # Build the query
            query = {
                'created_at': {'$gte': cutoff_time}
            }

            if domain:
                query['job_data.domain'] = domain
            elif url:
                query['job_data.url'] = url

            # Find the most recent job - use find with sort and limit
            result = self.mongodb_client.find(
                self.CRAWL_JOBS_COLLECTION,
                query,
                sort=[('created_at', -1)],
                limit=1
            )

            job = None
            if result and len(result) > 0:
                job = result[0]
                self.logger.info(f"Found existing job for {domain or url}: {job.get('crawl_id')} ({job.get('status')})")

            return job

        except Exception as e:
            self.logger.error(f"Error checking if job exists: {str(e)}")
            return None

    def create_crawl_job(self, job_data, job_id, initial_status='fresh'):
        """
        Create a new crawl job in the database

        Args:
            job_data (dict): Job data
            job_id (str): Job ID
            initial_status (str): Initial status for the job (default: 'fresh')

        Returns:
            str: Crawl ID
        """
        # Check if a similar job already exists and was recently created
        domain = job_data.get('domain')
        url = job_data.get('url')

        existing_job = self.check_job_exists(domain, url, timeframe_hours=24)
        if existing_job:
            # If job exists but is completed or failed, proceed with creating a new one
            if existing_job.get('status') in ['completed', 'failed']:
                self.logger.info(f"Creating new job for {domain or url} (previous job {existing_job.get('crawl_id')} was {existing_job.get('status')})")
            else:
                # Job is still pending or running, use the existing job
                self.logger.info(f"Returning existing job ID for {domain or url}: {existing_job.get('crawl_id')} ({existing_job.get('status')})")
                return existing_job.get('crawl_id')

        # Generate crawl ID
        crawl_id = str(uuid.uuid4())

        # Fill in any missing parameters from environment
        job_data = get_job_params(job_data)

        # Create job document
        job_doc = {
            'crawl_id': crawl_id,
            'job_id': job_id,
            'job_data': job_data,
            'status': initial_status,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }

        try:
            # Insert job
            self.mongodb_client.insert_one(self.CRAWL_JOBS_COLLECTION, job_doc)

            # Log with job ID context
            self.logger.info(format_log_message(
                f"Created crawl job (status: {initial_status})",
                crawl_id=crawl_id,
                job_id=job_id
            ))

            # Initialize stats
            self._initialize_crawl_stats(crawl_id, job_id, job_data)

            return crawl_id
        except Exception as e:
            log_exception(
                self.logger,
                e,
                "Failed to create crawl job",
                crawl_id=crawl_id
            )
            raise

    def _initialize_crawl_stats(self, crawl_id, job_id, job_data):
        """Initialize stats for a crawl job"""
        stats_doc = {
            'crawl_id': crawl_id,
            'job_id': job_id,
            'job_data': job_data,
            'start_time': datetime.utcnow(),
            'status': 'initializing',
            'pages_crawled': 0,
            'max_pages': job_data.get('max_pages', 50),
            'content_types': {},
            'status_codes': {},
            'errors': 0,
            'parse_jobs_created': 0,
            'progress': 0,
            'updated_at': datetime.utcnow()
        }

        try:
            self.mongodb_client.insert_one(self.CRAWL_STATS_COLLECTION, stats_doc)
            self.logger.info(format_log_message(
                "Initialized stats for crawl job",
                crawl_id=crawl_id
            ))
        except Exception as e:
            log_exception(
                self.logger,
                e,
                "Failed to initialize stats for crawl job",
                crawl_id=crawl_id
            )
            raise

    def update_job_status(self, crawl_id, status, message=None):
        """
        Update the status of a crawl job

        Args:
            crawl_id (str): Crawl ID
            status (str): New status ('pending', 'running', 'completed', 'failed')
            message (str, optional): Status message

        Returns:
            bool: True if updated, False otherwise
        """
        # Get a job-specific logger
        job_logger = get_job_specific_logger(
            self.logger,
            crawl_id=crawl_id,
            log_dir=LOG_DIR
        )

        update_data = {
            '$set': {
                'status': status,
                'updated_at': datetime.utcnow()
            }
        }

        if message:
            update_data['$set']['status_message'] = message

        try:
            # Update job status in crawl_jobs collection
            result = self.mongodb_client.update_one(
                self.CRAWL_JOBS_COLLECTION,
                {'crawl_id': crawl_id},
                update_data
            )

            # Also update status in stats collection
            stats_update = {'status': status, 'updated_at': datetime.utcnow()}

            # If job is completed, also update end_time and duration
            if status == 'completed':
                job = self.get_job(crawl_id)
                if job:
                    start_time = job.get('job_data', {}).get('start_time')
                    if not start_time:
                        start_time = job.get('created_at')

                    if start_time:
                        end_time = datetime.utcnow()
                        stats_update['end_time'] = end_time

                        # Calculate duration in seconds
                        if isinstance(start_time, str):
                            from dateutil import parser
                            start_time = parser.parse(start_time)

                        duration_seconds = (end_time - start_time).total_seconds()
                        stats_update['duration'] = duration_seconds

                        if message is None:
                            update_data['$set']['status_message'] = f"Crawler completed successfully in {duration_seconds:.2f} seconds"

            self.mongodb_client.update_one(
                self.CRAWL_STATS_COLLECTION,
                {'crawl_id': crawl_id},
                {'$set': stats_update}
            )

            updated = result.get('modified_count', 0) > 0

            if updated:
                log_msg = f"Updated job status to {status}"
                if message:
                    log_msg += f" with message: {message}"
                job_logger.info(log_msg)
            else:
                job_logger.warning(f"Status update to {status} had no effect")

            return updated
        except Exception as e:
            log_exception(
                job_logger,
                e,
                f"Failed to update job status to {status}",
                crawl_id=crawl_id
            )
            return False

    def update_crawl_stats(self, crawl_id, stats_update):
        """
        Update stats for a crawl job

        Args:
            crawl_id (str): Crawl ID
            stats_update (dict): Stats data to update

        Returns:
            bool: True if updated, False otherwise
        """
        # Get a job-specific logger
        job_logger = get_job_specific_logger(
            self.logger,
            crawl_id=crawl_id,
            log_dir=LOG_DIR
        )

        try:
            # Add updated_at timestamp
            stats_update['updated_at'] = datetime.utcnow()

            # Update the stats document
            result = self.mongodb_client.update_one(
                self.CRAWL_STATS_COLLECTION,
                {'crawl_id': crawl_id},
                {'$set': stats_update}
            )

            updated = result.get('modified_count', 0) > 0

            if not updated:
                job_logger.warning("Stats update had no effect")

            return updated
        except Exception as e:
            log_exception(
                job_logger,
                e,
                "Failed to update stats",
                crawl_id=crawl_id
            )
            return False

    def get_job(self, crawl_id):
        """
        Get a crawl job by ID

        Args:
            crawl_id (str): Crawl ID

        Returns:
            dict: Job document or None
        """
        try:
            return self.mongodb_client.find_one(
                self.CRAWL_JOBS_COLLECTION,
                {'crawl_id': crawl_id}
            )
        except Exception as e:
            self.logger.error(format_log_message(
                f"Failed to get job: {str(e)}",
                crawl_id=crawl_id
            ))
            return None

    def get_job_stats(self, crawl_id):
        """
        Get stats for a crawl job

        Args:
            crawl_id (str): Crawl ID

        Returns:
            dict: Stats document or None
        """
        try:
            return self.mongodb_client.find_one(
                self.CRAWL_STATS_COLLECTION,
                {'crawl_id': crawl_id}
            )
        except Exception as e:
            self.logger.error(format_log_message(
                f"Failed to get stats: {str(e)}",
                crawl_id=crawl_id
            ))
            return None

    def get_job_data(self, crawl_id):
        """
        Get job data for a specific crawl ID
        This is used by workers to fetch parameters after receiving a job from the queue

        Args:
            crawl_id (str): Crawl ID

        Returns:
            dict: Job data dictionary or None
        """
        try:
            job = self.get_job(crawl_id)
            if job and 'job_data' in job:
                return job['job_data']
            return None
        except Exception as e:
            self.logger.error(format_log_message(
                f"Failed to get job data: {str(e)}",
                crawl_id=crawl_id
            ))
            return None

    def get_pending_jobs(self, limit=10):
        """
        Get pending crawl jobs

        Args:
            limit (int, optional): Maximum number of jobs to return

        Returns:
            list: List of pending job documents
        """
        try:
            return self.mongodb_client.find(
                self.CRAWL_JOBS_COLLECTION,
                {'status': 'pending'},
                sort=[('created_at', ASCENDING)],
                limit=limit
            )
        except Exception as e:
            self.logger.error(f"Failed to get pending jobs: {str(e)}")
            return []

    def get_running_jobs(self, limit=10):
        """
        Get running crawl jobs

        Args:
            limit (int, optional): Maximum number of jobs to return

        Returns:
            list: List of running job documents
        """
        try:
            return self.mongodb_client.find(
                self.CRAWL_JOBS_COLLECTION,
                {'status': 'running'},
                sort=[('updated_at', ASCENDING)],
                limit=limit
            )
        except Exception as e:
            self.logger.error(f"Failed to get running jobs: {str(e)}")
            return []

    def get_recent_jobs(self, limit=10):
        """
        Get recent crawl jobs

        Args:
            limit (int, optional): Maximum number of jobs to return

        Returns:
            list: List of recent job documents
        """
        try:
            return self.mongodb_client.find(
                self.CRAWL_JOBS_COLLECTION,
                {},
                sort=[('created_at', DESCENDING)],
                limit=limit
            )
        except Exception as e:
            self.logger.error(f"Failed to get recent jobs: {str(e)}")
            return []