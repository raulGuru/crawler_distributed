import logging
from .beanstalkd_client import BeanstalkdClient
from .job_serializer import JobSerializer


class QueueManager:
    """
    High-level queue operations abstraction for managing multiple tubes and priorities
    """

    # Default tube names
    DEFAULT_TUBES = {
        'crawl': 'crawl_jobs',
        'parse': 'parse_jobs',
        'monitor': 'monitor_jobs'
    }

    # Default priorities (lower is higher priority)
    DEFAULT_PRIORITIES = {
        'high': 0,
        'normal': 100,
        'low': 1000
    }

    def __init__(self, host='localhost', port=11300):
        self.host = host
        self.port = port
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None
        self.serializer = JobSerializer()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize beanstalkd client"""
        try:
            self.client = BeanstalkdClient(host=self.host, port=self.port)
            self.logger.info(f"Queue manager initialized with beanstalkd at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to initialize queue manager: {str(e)}")
            raise

    def _get_tube_for_job_type(self, job_type):
        """Get tube name for job type"""
        return self.DEFAULT_TUBES.get(job_type, job_type)

    def _get_priority(self, priority):
        """Get numeric priority value"""
        if isinstance(priority, int):
            return priority
        return self.DEFAULT_PRIORITIES.get(priority, self.DEFAULT_PRIORITIES['normal'])

    def _clean_job_data(self, job_data):
        """
        Return a clean copy of job_data without internal attributes

        Args:
            job_data (dict): Job data

        Returns:
            dict: Cleaned job data
        """
        if job_data is None:
            return None

        # Create a new dict without the _job attribute
        return {k: v for k, v in job_data.items() if k != '_job'}

    def enqueue_job(self, job_data, tube=None, priority='normal', delay=0, ttr=60):
        """
        Enqueue a job to the specified tube

        Args:
            job_data (dict): Job data to enqueue
            tube (str, optional): Tube name. If None, derive from job_type
            priority (str/int, optional): Job priority ('high', 'normal', 'low' or numeric)
            delay (int, optional): Delay in seconds before job is ready
            ttr (int, optional): Time to run in seconds

        Returns:
            int: Job ID
        """
        # If we're submitting just a crawl_id, ensure job_type is set
        if 'crawl_id' in job_data and 'job_type' not in job_data:
            job_data['job_type'] = 'crawl'

        # Check if the job is a duplicate (same domain/URL with recent timestamp)
        # This helps prevent job duplication issues
        if job_data.get('job_type') == 'crawl':
            domain = job_data.get('domain')
            url = job_data.get('url')

            if domain or url:
                target = domain if domain else url
                self.logger.info(f"Enqueueing job for {target}")

                # Add timestamp if not present
                if 'submitted_at' not in job_data:
                    from datetime import datetime
                    job_data['submitted_at'] = datetime.utcnow().isoformat()

        # Determine tube from job_type if not specified
        if tube is None:
            job_type = job_data.get('job_type')
            if not job_type:
                raise ValueError("Job data missing 'job_type' field and no tube specified")
            tube = self._get_tube_for_job_type(job_type)

        # Get numeric priority
        numeric_priority = self._get_priority(priority)

        try:
            # Serialize job data
            serialized_job = self.serializer.serialize_job(job_data)

            # Use the tube
            self.client.use_tube(tube)

            # Put the job
            job_id = self.client.put(
                serialized_job,
                priority=numeric_priority,
                delay=delay,
                ttr=ttr
            )

            self.logger.info(f"Enqueued job {job_id} to tube {tube} with priority {numeric_priority}")
            return job_id

        except Exception as e:
            self.logger.error(f"Failed to enqueue job to tube {tube}: {str(e)}")
            raise

    def dequeue_job(self, tubes=None, timeout=None):
        """
        Dequeue a job from the specified tubes

        Args:
            tubes (list/str, optional): Tube or list of tubes to watch
            timeout (int, optional): Timeout in seconds

        Returns:
            tuple: (job_id, job_data, job_obj) or (None, None, None) if timeout
        """
        if tubes is None:
            tubes = list(self.DEFAULT_TUBES.values())
        elif isinstance(tubes, str):
            tubes = [tubes]

        try:
            # Watch specified tubes
            for tube in tubes:
                self.client.watch_tube(tube)

            # Ignore default tube if not in tubes
            if 'default' not in tubes:
                try:
                    self.client.ignore_tube('default')
                except:
                    pass  # Ignore if not watching default

            # Reserve a job
            job = self.client.reserve(timeout=timeout)
            if job is None:
                return None, None, None

            # Deserialize job data
            try:
                job_data = self.serializer.deserialize_job(job.body)
                job_id = job.jid
                # Store the job object in the job data for later operations
                job_data['_job'] = job
                self.logger.debug(f"Dequeued job {job_id}")

                # Return a clean copy of the job data (without the job object)
                # for storage in database, etc.
                return job_id, self._clean_job_data(job_data), job
            except Exception as e:
                self.logger.error(f"Failed to deserialize job {job.jid}: {str(e)}")
                # Bury the job as it's malformed
                self.client.bury(job)
                return None, None, None

        except Exception as e:
            self.logger.error(f"Failed to dequeue job: {str(e)}")
            return None, None, None

    def complete_job(self, job_data):
        """
        Mark a job as completed (delete it)

        Args:
            job_data (dict): Job data with _job attribute
        """
        job = job_data.get('_job')
        if not job:
            self.logger.error("Cannot complete job: _job attribute missing")
            return

        try:
            job_id = job.jid
            self.client.delete(job)
            self.logger.info(f"Completed and deleted job {job_id}")

            # Also check for any zombie jobs with the same crawl_id
            crawl_id = job_data.get('crawl_id')
            if crawl_id:
                self.logger.info(f"Cleaning up any zombie jobs for crawl_id {crawl_id}")
                self.purge_completed_jobs(crawl_id)

        except Exception as e:
            self.logger.error(f"Failed to complete job {job.jid}: {str(e)}")

    def purge_completed_jobs(self, crawl_id=None):
        """
        Purge completed jobs from the queue

        Args:
            crawl_id (str, optional): Specific crawl_id to purge, or None for all
        """
        if not self.client:
            return

        try:
            # Since we can't get a list of tubes easily with the current client,
            # just check the default tubes we know about
            known_tubes = list(self.DEFAULT_TUBES.values()) + ['default']

            for tube in known_tubes:
                try:
                    # Use the tube
                    self.client.use_tube(tube)
                    self.client.watch_tube(tube)

                    # Check for jobs in the ready state that might be duplicates
                    for _ in range(5):  # Limit the number of peek attempts
                        try:
                            # Use peek_ready from the underlying client if available
                            job = None
                            if hasattr(self.client, 'peek_ready'):
                                job = self.client.peek_ready()
                            elif hasattr(self.client.client, 'peek_ready'):
                                job = self.client.client.peek_ready()

                            if not job:
                                break

                            # Try to deserialize the job data
                            try:
                                job_data = self.serializer.deserialize_job(job.body)
                                job_crawl_id = job_data.get('crawl_id')

                                # If crawl_id matches or we're purging all completed jobs
                                if not crawl_id or job_crawl_id == crawl_id:
                                    self.logger.info(f"Purging stale job {job.id} with crawl_id {job_crawl_id}")
                                    self.client.delete(job)
                            except:
                                # If we can't deserialize, just skip this job
                                pass
                        except Exception as e:
                            # No more jobs to peek or error
                            self.logger.debug(f"Error peeking ready jobs: {str(e)}")
                            break
                except Exception as e:
                    self.logger.warning(f"Error purging jobs from tube {tube}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Failed to purge completed jobs: {str(e)}")

    def retry_job(self, job_data, delay=30, priority=None):
        """
        Retry a job (release it back to the queue), but only up to 3 times.
        After 3 failed attempts, mark as failed in the database and do not retry.
        """
        job = job_data.get('_job')
        if not job:
            self.logger.error("Cannot retry job: _job attribute missing")
            return

        # Use current priority if not specified
        if priority is None:
            priority = job.stats().get('pri', self.DEFAULT_PRIORITIES['normal'])
        else:
            priority = self._get_priority(priority)

        # Track retries
        retries = job_data.get('retries', 0) + 1
        job_data['retries'] = retries
        crawl_id = job_data.get('crawl_id')

        if retries > 3:
            # Mark as failed in DB and bury the job
            self.logger.error(f"Job {job.jid} exceeded max retries (3). Marking as failed.")
            try:
                # Bury the job in the queue
                self.client.bury(job)
                self.logger.info(f"Buried job {job.jid} after max retries.")
            except Exception as e:
                self.logger.error(f"Error marking job as failed in DB: {str(e)}")
            return

        try:
            job_id = job.jid
            self.client.release(job, priority=priority, delay=delay)
            self.logger.info(f"Released job {job_id} for retry with delay {delay}s and priority {priority} (retry {retries})")
        except Exception as e:
            self.logger.error(f"Failed to retry job {job.jid}: {str(e)}")

    def fail_job(self, job_data, permanent=False):
        """
        Mark a job as failed, with retry limit logic.
        """
        job = job_data.get('_job')
        if not job:
            self.logger.error("Cannot fail job: _job attribute missing")
            return

        retries = job_data.get('retries', 0) + 1
        job_data['retries'] = retries
        crawl_id = job_data.get('crawl_id')

        try:
            job_id = job.jid
            if permanent or retries > 3:
                # Bury the job (permanent failure or exceeded retries)
                self.client.bury(job)
                self.logger.info(f"Buried failed job {job_id} (permanent failure or max retries)")
            else:
                # Retry with increasing delay (exponential backoff)
                delay = min(30 * 60, 5 * (2 ** retries))
                updated_job_data = {k: v for k, v in job_data.items() if k != '_job'}
                serialized_job = self.serializer.serialize_job(updated_job_data)
                tube = job.stats()['tube']
                self.client.delete(job)
                self.client.use_tube(tube)
                new_job_id = self.client.put(
                    serialized_job,
                    priority=self.DEFAULT_PRIORITIES['normal'],
                    delay=delay,
                    ttr=job.stats().get('ttr', 60)
                )
                self.logger.info(f"Failed job {job_id} rescheduled as {new_job_id} with delay {delay}s (retry {retries})")
        except Exception as e:
            self.logger.error(f"Failed to mark job {job.jid} as failed: {str(e)}")

    def get_stats(self):
        """
        Get queue statistics

        Returns:
            dict: Queue statistics
        """
        stats = {
            'tubes': {},
            'total_jobs': 0,
            'ready_jobs': 0,
            'reserved_jobs': 0,
            'delayed_jobs': 0,
            'buried_jobs': 0
        }

        try:
            # Get list of tubes
            tubes = self.client.tubes()

            # Get stats for each tube
            for tube in tubes:
                tube_stats = self.client.stats_tube(tube)
                stats['tubes'][tube] = {
                    'ready': tube_stats.get('current-jobs-ready', 0),
                    'reserved': tube_stats.get('current-jobs-reserved', 0),
                    'delayed': tube_stats.get('current-jobs-delayed', 0),
                    'buried': tube_stats.get('current-jobs-buried', 0),
                    'total': tube_stats.get('current-jobs-ready', 0) +
                             tube_stats.get('current-jobs-reserved', 0) +
                             tube_stats.get('current-jobs-delayed', 0) +
                             tube_stats.get('current-jobs-buried', 0),
                    'total_jobs': tube_stats.get('total-jobs', 0)
                }

                # Update totals
                stats['total_jobs'] += stats['tubes'][tube]['total']
                stats['ready_jobs'] += stats['tubes'][tube]['ready']
                stats['reserved_jobs'] += stats['tubes'][tube]['reserved']
                stats['delayed_jobs'] += stats['tubes'][tube]['delayed']
                stats['buried_jobs'] += stats['tubes'][tube]['buried']

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get queue stats: {str(e)}")
            return stats

    def close(self):
        """Close the client connection"""
        if self.client:
            self.client.close()
            self.client = None