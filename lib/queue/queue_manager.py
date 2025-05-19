from contextlib import closing
import logging
from greenstalk import NotIgnoredError
from datetime import datetime

from config.base_settings import QUEUE_CRAWL_TUBE, QUEUE_PARSE_TUBE, QUEUE_MONITOR_TUBE, MONGO_CRAWL_JOB_COLLECTION
from .beanstalkd_client import BeanstalkdClient
from .job_serializer import JobSerializer


class QueueManager:
    """
    High-level queue operations abstraction for managing multiple tubes and priorities
    """

    # Default tube names
    DEFAULT_TUBES = {
        'crawl': QUEUE_CRAWL_TUBE,
        'parse': QUEUE_PARSE_TUBE,
        'monitor': QUEUE_MONITOR_TUBE
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
            job_data (dict): Job data to enqueue (assumed to be fully prepared by caller)
            tube (str, optional): Tube name. If None, derive from job_type using DEFAULT_TUBES.
            priority (str/int, optional): Job priority ('high', 'normal', 'low' or numeric)
            delay (int, optional): Delay in seconds before job is ready
            ttr (int, optional): Time to run in seconds

        Returns:
            int: Job ID from Beanstalkd, or None if enqueueing failed before Beanstalkd operation.
        """
        current_tube = tube
        if current_tube is None:
            job_type = job_data.get('job_type')
            if not job_type:
                self.logger.error("Job data missing 'job_type' and no explicit 'tube' specified for enqueue_job.")
                raise ValueError("Job data missing 'job_type' and no explicit 'tube' specified.")
            current_tube = self._get_tube_for_job_type(job_type)
            if current_tube == job_type:
                self.logger.warning(f"No default tube configured for job_type '{job_type}'. Using job_type name as tube name: '{current_tube}'. Explicitly pass 'tube' to enqueue_job if this is not intended.")

        numeric_priority = self._get_priority(priority)

        try:
            serialized_job_str = self.serializer.serialize_job(job_data)
            serialized_job_bytes = serialized_job_str.encode('utf-8')

            self.use_tube(current_tube)

            job_id = self.client.put(
                serialized_job_bytes,
                priority=numeric_priority,
                delay=delay,
                ttr=ttr
            )

            self.logger.info(f"Enqueued job {job_id} to tube {current_tube} with priority {numeric_priority}, TTR {ttr}s, delay {delay}s.")
            return job_id

        except Exception as e:
            self.logger.error(f"Failed to enqueue job to tube {current_tube}: {str(e)}")
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
            for t in tubes:
                self.watch_tube(t)

            # Reserve a job
            job = self.client.reserve(timeout=timeout)
            if job is None:
                return None, None, None

            # Deserialize job data
            try:
                job_body_str = None
                if isinstance(job.body, bytes):
                    job_body_str = job.body.decode('utf-8')
                elif isinstance(job.body, str):
                    # This might be an old job that was enqueued as a string
                    self.logger.warning(f"Job {job.jid} body is already a string. Proceeding with deserialization. This might indicate an old job format.")
                    job_body_str = job.body
                else:
                    raise ValueError(f"Unexpected type for job.body: {type(job.body)}")

                job_data = self.serializer.deserialize_job(job_body_str)
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

    def complete_job(self, job_object, job_data_dict=None):
        """
        Mark a job as completed (delete it).

        Args:
            job_object: The job object obtained from dequeue_job.
            job_data_dict (dict, optional): The original deserialized job data dictionary, for logging or context.
        """
        if not job_object:
            self.logger.error("Cannot complete job: job_object is missing")
            return

        try:
            job_id = job_object.id
            self.delete_job(job_object)
            self.logger.info(f"Completed and deleted job {job_id} via complete_job.")

            # Also check for any zombie jobs with the same crawl_id
            if job_data_dict and job_data_dict.get('crawl_id'):
                crawl_id = job_data_dict.get('crawl_id')
                self.logger.info(f"Cleaning up any zombie jobs for crawl_id {crawl_id}")
                self.purge_completed_jobs(crawl_id)
            elif job_data_dict is None:
                self.logger.debug("job_data_dict not provided to complete_job, skipping zombie job check for crawl_id.")

        except Exception as e:
            self.logger.error(f"Failed to complete job {getattr(job_object, 'id', 'unknown')}: {str(e)}")

    def touch_job(self, job_object):
        """Touch a reserved job to extend its TTR. Expects a Beanstalkd job object."""
        if not job_object or not (hasattr(job_object, 'id') and hasattr(job_object, 'body')):
            # Check if it looks like a greenstalk job object
            self.logger.error(
                f"Cannot touch job: job_object is missing or not a valid job object. Type: {type(job_object)}, Value: {str(job_object)[:100]}"
            )
            raise ValueError("job_object must be a valid Beanstalkd job object")
        try:
            self.logger.debug(f"QueueManager: Attempting to call self.client.touch for job_object with id {getattr(job_object, 'id', 'N/A')}. Type of job_object: {type(job_object)}")
            self.client.touch(job_object) # Pass the full job object as BeanstalkdClient can handle it
            self.logger.info(f"Touched job {job_object.id} via QueueManager.")
        except Exception as e:
            job_id_for_log = 'unknown_job_id'
            if hasattr(job_object, 'id'):
                job_id_for_log = job_object.id
            elif isinstance(job_object, int):
                job_id_for_log = job_object
            self.logger.error(f"QueueManager failed to touch job {job_id_for_log} (job_object type: {type(job_object)}): {repr(e)}")
            raise

    def get_job_stats(self, job_object) -> dict | None:
        """Get stats for a job object. Expects a Beanstalkd job object."""
        if not job_object or not (hasattr(job_object, 'id') and hasattr(job_object, 'body')):
            self.logger.error("Cannot get job stats: job_object is missing or not a valid job object")
            raise ValueError("job_object must be a valid Beanstalkd job object")
        try:
            return self.client.get_job_stats(job_object)
        except Exception as e:
            job_id = getattr(job_object, 'id', 'unknown')
            self.logger.error(f"QueueManager failed to get stats for job {job_id}: {e}")
            return None # Or raise

    def purge_completed_jobs(self, crawl_id: str | None = None) -> None:
        """Call the safe helper for each known tube."""
        for tube in (
            QUEUE_CRAWL_TUBE,
            QUEUE_PARSE_TUBE,
            QUEUE_MONITOR_TUBE,
        ):
            try:
                self._purge_with_fresh_client(tube, crawl_id)
            except Exception as exc:
                self.logger.error(f"Purge error in tube {tube}: {exc}")

    def _purge_with_fresh_client(self, tube: str, crawl_id: str | None) -> None:
        """Delete READY jobs that match *crawl_id* using a short-lived connection."""
        with closing(BeanstalkdClient(self.host, self.port)) as tmp:
            tmp.watch_tube(tube)
            try:
                tmp.ignore_tube("default")
            except NotIgnoredError:
                pass

            for _ in range(10):
                job = tmp.peek_ready(tube)
                if not job:
                    break

                body = job.body.decode() if isinstance(job.body, bytes) else job.body
                try:
                    data = self.serializer.deserialize_job(body)
                except Exception:
                    tmp.delete(job)
                    continue
                if crawl_id is None or data.get("crawl_id") == crawl_id:
                    tmp.delete(job)

    def purge_completed_jobs_old(self, crawl_id=None):
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

    def retry_job(self, job_object, job_data_dict, delay=30, priority_str_or_int=None):
        """
        Retry a job (release it back to the queue), but only up to 3 times.
        After 3 failed attempts, bury the job.
        Note: Beanstalkd 'release' does not modify the job body in the queue.
              The 'retries' count in job_data_dict is an in-memory update for logging/logic.

        Args:
            job_object: The job object obtained from dequeue_job.
            job_data_dict (dict): The deserialized job data dictionary.
            delay (int, optional): Delay in seconds before job is ready again.
            priority_str_or_int (str/int, optional): Job priority for re-queue.
        """
        if not job_object:
            self.logger.error("Cannot retry job: job_object is missing")
            return
        if job_data_dict is None:
            self.logger.error("Cannot retry job: job_data_dict is missing (needed for retry count)")
            return

        job_id = job_object.id

        # Use current priority if not specified, by peeking job stats from the object
        current_priority_from_stats = job_object.stats().get('pri', self.DEFAULT_PRIORITIES['normal'])
        numeric_priority = self._get_priority(priority_str_or_int if priority_str_or_int is not None else current_priority_from_stats)

        # Track retries
        retries = job_data_dict.get('retries', 0) + 1
        job_data_dict['retries'] = retries

        if retries > 3:
            self.logger.error(f"Job {job_id} (crawl_id: {job_data_dict.get('crawl_id', 'N/A')}) exceeded max retries ({retries-1}). Burying job.")
            try:
                self.client.bury(job_object)
                self.logger.info(f"Buried job {job_id} after max retries.")
            except Exception as e:
                self.logger.error(f"Error burying job {job_id} after max retries: {str(e)}")
            return

        try:
            self.client.release(job_object, priority=numeric_priority, delay=delay)
            self.logger.info(
                f"Released job {job_id} (crawl_id: {job_data_dict.get('crawl_id', 'N/A')}) for retry {retries} "
                f"with delay {delay}s and priority {numeric_priority}."
            )
        except Exception as e:
            self.logger.error(f"Failed to retry/release job {job_id}: {str(e)}")

    def bury_job(self, job_object, job_data_dict, priority_str_or_int='normal'):
        """Bury a job so it's not processed further without intervention. Expects a Beanstalkd job object."""
        if not job_object or not (hasattr(job_object, 'id') and hasattr(job_object, 'body')):
            self.logger.error("Cannot bury job: job_object is missing or not a valid job object")
            raise ValueError("job_object must be a valid Beanstalkd job object")

        numeric_priority = self._get_priority(priority_str_or_int)
        job_id_to_bury = getattr(job_object, 'id', 'unknown')
        mongodb_collection_name = job_data_dict.get('_meta', {}).get('mongo_collection', MONGO_CRAWL_JOB_COLLECTION)

        try:
            self.client.bury(job_object, priority=numeric_priority) # Pass the full job object
            self.logger.info(f"Buried job {job_id_to_bury} with priority {numeric_priority}.")

            # Update MongoDB status to 'buried' or similar
            if job_data_dict and job_data_dict.get('crawl_id') and hasattr(self, 'mongodb_client') and self.mongodb_client:
                self.logger.info(f"Attempting to update MongoDB status to 'buried' for crawl_id {job_data_dict.get('crawl_id')}")
                self.mongodb_client.update_one(
                    mongodb_collection_name, # Use actual collection name
                    {'crawl_id': job_data_dict.get('crawl_id')},
                    {'$set': {'crawl_status': 'buried_max_retries', 'updated_at': datetime.utcnow()}}
                )
                self.logger.info(f"Updated MongoDB status to 'buried_max_retries' for crawl_id {job_data_dict.get('crawl_id')}")
            elif not (hasattr(self, 'mongodb_client') and self.mongodb_client):
                self.logger.warning(f"MongoDB client not available in QueueManager, cannot update status for buried job {job_id_to_bury}.")

        except Exception as e:
            self.logger.error(f"QueueManager failed to bury job {job_id_to_bury}: {str(e)}")
            # Decide if to re-raise or not. If burying fails, it's tricky.
            raise

    def fail_job(self, job_object, job_data_dict, permanent=False):
        """
        Mark a job as failed.
        If not permanent and retries < max_retries, it re-queues the job with a delay.
        Otherwise, it buries the job.

        Args:
            job_object: The job object obtained from dequeue_job.
            job_data_dict (dict): The deserialized job data dictionary.
            permanent (bool, optional): If True, bury immediately. Defaults to False.
        """
        if not job_object:
            self.logger.error("Cannot fail job: job_object is missing")
            return
        if job_data_dict is None:
            self.logger.error("Cannot fail job: job_data_dict is missing (needed for retry count and re-queueing)")
            return

        job_id = job_object.id
        retries = job_data_dict.get('retries', 0) + 1
        job_data_dict['retries'] = retries

        crawl_id_log = job_data_dict.get('crawl_id', 'N/A')

        try:
            if permanent or retries > 3:
                self.logger.info(f"Burying failed job {job_id} (crawl_id: {crawl_id_log}). Permanent failure or max retries ({retries-1}) exceeded.")
                self.client.bury(job_object)
            else:
                delay = min(30 * 60, 5 * (2 ** retries))

                serialized_new_job_body_str = self.serializer.serialize_job(job_data_dict)
                serialized_new_job_body_bytes = serialized_new_job_body_str.encode('utf-8')

                original_tube = job_object.stats().get('tube', self.DEFAULT_TUBES.get(job_data_dict.get('job_type'), 'default'))
                original_ttr = job_object.stats().get('ttr', 60)
                original_priority = job_object.stats().get('pri', self.DEFAULT_PRIORITIES['normal'])

                self.client.delete(job_object)
                self.logger.debug(f"Deleted old job instance {job_id} before re-queueing failed job.")

                self.client.use_tube(original_tube)
                new_job_id = self.client.put(
                    serialized_new_job_body_bytes,
                    priority=original_priority,
                    delay=delay,
                    ttr=original_ttr
                )
                self.logger.info(
                    f"Failed job {job_id} (crawl_id: {crawl_id_log}) was re-queued as new job {new_job_id} in tube '{original_tube}' "
                    f"with delay {delay}s (retry attempt {retries})."
                )
        except Exception as e:
            self.logger.error(f"Error processing fail_job for job {job_id} (crawl_id: {crawl_id_log}): {str(e)}. Attempting to bury original job as a fallback.")
            try:
                if not (permanent or retries > 3):
                    stats = job_object.stats()
                    if stats and stats.get('state') != 'buried':
                        self.client.bury(job_object)
                        self.logger.info(f"Fallback: Buried original job {job_id} due to error during fail_job processing.")
            except Exception as bury_e:
                self.logger.error(f"Fallback bury attempt for job {job_id} also failed: {str(bury_e)}")

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

    def get_detailed_monitoring_view(self):
        """
        Provides a detailed, comprehensive view of the Beanstalkd instance,
        including server stats, stats for each tube, and a peek at the next
        ready, delayed, and buried job in each tube.
        """
        if not self.client:
            self.logger.error("Beanstalkd client not initialized. Cannot get detailed monitoring view.")
            return {"error": "Client not initialized"}

        detailed_stats = {
            'server_stats': None,
            'tubes_details': {}
        }

        try:
            detailed_stats['server_stats'] = self.client.stats()
        except Exception as e:
            self.logger.error(f"Failed to get server stats: {str(e)}")
            detailed_stats['server_stats'] = {"error": str(e)}

        try:
            tubes = self.client.tubes()
        except Exception as e:
            self.logger.error(f"Failed to list tubes: {str(e)}")
            detailed_stats['error_listing_tubes'] = str(e)
            return detailed_stats

        for tube_name in tubes:
            tube_info = {
                'stats': None,
                'peeked_ready_job': None,
                'peeked_delayed_job': None,
                'peeked_buried_job': None
            }
            try:
                tube_info['stats'] = self.client.stats_tube(tube_name)
            except Exception as e:
                self.logger.error(f"Failed to get stats for tube {tube_name}: {str(e)}")
                tube_info['stats'] = {"error": str(e)}

            peek_methods = {
                'peeked_ready_job': self.client.peek_ready,
                'peeked_delayed_job': self.client.peek_delayed,
                'peeked_buried_job': self.client.peek_buried
            }

            for job_state_key, peek_method in peek_methods.items():
                try:
                    # The peek methods in BeanstalkdClient already use the correct tube.
                    job_object = peek_method(tube_name)
                    if job_object:
                        job_details = {'id': job_object.id, 'body': None, 'job_stats': None}
                        try:
                            # Ensure body is string before deserializing
                            body_str = job_object.body
                            if isinstance(body_str, bytes):
                                body_str = body_str.decode('utf-8', errors='replace') # Handle potential decoding errors

                            # Use _clean_job_data to avoid including internal attributes like '_job'
                            deserialized_data = self.serializer.deserialize_job(body_str)
                            job_details['body'] = self._clean_job_data(deserialized_data)
                        except Exception as deserialize_e:
                            self.logger.warning(f"Failed to deserialize job {job_object.id} in tube {tube_name} ({job_state_key}): {str(deserialize_e)}")
                            job_details['body'] = {"error_deserializing": str(deserialize_e), "raw_body": body_str if 'body_str' in locals() else job_object.body}

                        try:
                            # Access stats directly from the underlying greenstalk connection stats_job
                            if self.client.connection:
                                job_details['job_stats'] = self.client.connection.stats_job(job_object.id)
                            else:
                                job_details['job_stats'] = {"error": "Client connection not available for job stats"}
                        except Exception as job_stats_e:
                            self.logger.warning(f"Failed to get stats for job {job_object.id} in tube {tube_name}: {str(job_stats_e)}")
                            job_details['job_stats'] = {"error": str(job_stats_e)}
                        tube_info[job_state_key] = job_details
                except Exception as peek_e:
                    self.logger.error(f"Failed to {job_state_key} for tube {tube_name}: {str(peek_e)}")
                    tube_info[job_state_key] = {"error": str(peek_e)}

            detailed_stats['tubes_details'][tube_name] = tube_info

        return detailed_stats

    def close(self):
        """Close the client connection"""
        if self.client:
            self.client.close()
            self.client = None

    def use_tube(self, tube_name: str):
        """
        Select a tube for subsequent 'put' operations.
        This will also create the tube if it doesn't exist on the server.
        """
        try:
            self.client.use_tube(tube_name)
        except Exception as e:
            self.logger.error(f"QueueManager: Failed to use tube {tube_name}: {str(e)}")
            raise

    def watch_tube(self, tube_name: str):
        """
        Add the given tube to the watch list for the current connection.
        'reserve' will take jobs from any of the watched tubes.
        """
        try:
            return self.client.watch_tube(tube_name)
        except Exception as e:
            self.logger.error(f"QueueManager: Failed to watch tube {tube_name}: {str(e)}")
            raise

    def ignore_tube(self, tube_name: str):
        """
        Stop watching the given tube.
        """
        try:
            return self.client.ignore_tube(tube_name)
        except Exception as e:
            self.logger.error(f"QueueManager: Failed to ignore tube {tube_name}: {str(e)}")
            raise

    def delete_job(self, job_obj):
        """
        Delete a job from the queue.
        Requires the job object obtained from dequeue_job.
        """
        if not job_obj:
            self.logger.error("Cannot delete job: job object is missing")
            return
        try:
            job_id = job_obj.id
            self.client.delete(job_obj)
            self.logger.info(f"Deleted job {job_id} directly.")
        except Exception as e:
            self.logger.error(f"Failed to delete job {getattr(job_obj, 'id', 'unknown')}: {str(e)}")
            # Potentially re-raise if critical
            # raise