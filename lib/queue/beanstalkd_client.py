import logging
import greenstalk


class BeanstalkdClient:
    """
    Wrapper for beanstalkd client with connection pooling and error handling
    """

    def __init__(self, host='localhost', port=11300, connect_timeout=5):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        self.connection = None
        self.current_tube = None
        self._connect()

    def _connect(self):
        """Establish connection to beanstalkd server"""
        try:
            # The greenstalk Client doesn't have a timeout parameter
            # We'll have to handle connection timeouts differently
            self.connection = greenstalk.Client(
                (self.host, self.port),
                watch=['default'],
                use='default'
            )
            self.logger.info(f"Connected to beanstalkd at {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to beanstalkd: {str(e)}")
            self.connection = None
            raise

    def _ensure_connection(self):
        """Ensure connection is established, reconnect if needed"""
        if not self.connection:
            self.logger.warning("Beanstalkd connection not established, reconnecting...")
            self._connect()

        # Reselect current tube if it was set
        if self.connection and self.current_tube:
            try:
                self.connection.use(self.current_tube)
            except Exception as e:
                self.logger.error(f"Failed to select tube {self.current_tube}: {str(e)}")
                self.connection = None
                raise

    def use_tube(self, tube):
        """Select tube for putting jobs"""
        self._ensure_connection()
        try:
            self.connection.use(tube)
            self.current_tube = tube
            self.logger.debug(f"Using tube: {tube}")
        except Exception as e:
            self.logger.error(f"Failed to use tube {tube}: {str(e)}")
            self.connection = None
            raise

    def watch_tube(self, tube):
        """Watch tube for reserving jobs"""
        self._ensure_connection()
        try:
            self.connection.watch(tube)
            # In greenstalk 2.0.2, we can't directly get the number of watched tubes
            # So we'll return a static value
            tubes_watched = 1
            self.logger.debug(f"Watching tube: {tube}")
            return tubes_watched
        except Exception as e:
            self.logger.error(f"Failed to watch tube {tube}: {str(e)}")
            self.connection = None
            raise

    def ignore_tube(self, tube):
        """Stop watching a tube"""
        self._ensure_connection()
        try:
            self.connection.ignore(tube)
            # In greenstalk 2.0.2, we can't directly get the number of watched tubes
            # So we'll return a static value
            watched_tubes = 1
            self.logger.debug(f"Ignoring tube: {tube}")
            return watched_tubes
        except Exception as e:
            self.logger.error(f"Failed to ignore tube {tube}: {str(e)}")
            raise

    def put(self, data, priority=1000, delay=0, ttr=60):
        """Put a job into the currently used tube"""
        self._ensure_connection()
        try:
            job_id = self.connection.put(
                data,
                priority=priority,
                delay=delay,
                ttr=ttr
            )
            self.logger.debug(f"Put job {job_id} into tube {self.current_tube}")
            return job_id
        except Exception as e:
            self.logger.error(f"Failed to put job: {str(e)}")
            raise

    def reserve(self, timeout=None):
        """Reserve a job from watched tubes"""
        self._ensure_connection()
        try:
            job = self.connection.reserve(timeout=timeout)
            if job:
                # In greenstalk, create a custom job object to match the old API
                job_stats = self.connection.stats_job(job.id)
                job.jid = job.id  # Add jid attribute for compatibility
                job.stats = lambda: job_stats  # Add stats method for compatibility
                self.logger.debug(f"Reserved job {job.id}")
            return job
        except greenstalk.TimedOutError:
            # This is an expected case when timeout is reached
            return None
        except Exception as e:
            self.logger.error(f"Failed to reserve job: {str(e)}")
            self.connection = None
            raise

    def delete(self, job_id: int):
        """Delete a job by its ID"""
        self._ensure_connection()
        try:
            self.connection.delete(job_id)
            self.logger.debug(f"Deleted job {job_id}")
        except Exception as e:
            self.logger.error(f"Failed to delete job {job_id}: {str(e)}")
            raise

    def touch(self, job_obj_or_id):
        """Touch a job to extend its TTR. Accepts Job object or job ID."""
        job_id_for_logging = getattr(job_obj_or_id, 'id', job_obj_or_id)
        try:
            # greenstalk.Client.touch() can accept either a Job object or an int ID.
            # Let's prefer passing the Job object if that's what we received,
            # otherwise, pass the ID.
            arg_to_touch = job_obj_or_id # Assume it's a Job object or an ID that greenstalk can handle
            self.logger.debug(f"BeanstalkdClient: Calling self.connection.touch with argument of type {type(arg_to_touch)} (job_id_for_logging: {job_id_for_logging})")
            self.connection.touch(arg_to_touch)
            self.logger.debug(f"Touched job {job_id_for_logging}")
        except greenstalk.UnknownResponseError as e:
            # This can happen if the job TTRs and is deleted before touch completes
            self.logger.warning(f"Failed to touch job {job_id_for_logging} (may have expired or been deleted): {e}")
            # Do not re-raise, allow main process to discover job is gone if needed
        except AttributeError as ae:
            # Check if the AttributeError is specifically "'int' object has no attribute 'id'"
            # and originates from the greenstalk call.
            if "'int' object has no attribute 'id'" in str(ae):
                self.logger.error(
                    f"BeanstalkdClient: Encountered AttributeError ('{str(ae)}') from greenstalk.connection.touch({job_id_for_logging}). "
                    f"This may indicate an issue within the greenstalk library. Job will not be touched."
                )
                # Do NOT re-raise this specific AttributeError to allow the toucher thread to continue attempting.
                # However, the touch did NOT succeed.
            else:
                self.logger.error(f"BeanstalkdClient: General AttributeError touching job {job_id_for_logging}: {repr(ae)}")
                raise # Re-raise other AttributeErrors
        except Exception as e:
            self.logger.error(f"BeanstalkdClient: General error touching job {job_id_for_logging}: {repr(e)}")
            raise # Re-raise other errors

    def release(self, job, priority=1000, delay=0):
        """Release a job back to the queue"""
        self._ensure_connection()
        try:
            self.connection.release(job.id, priority=priority, delay=delay)
            self.logger.debug(f"Released job {job.id} with priority {priority} and delay {delay}")
        except Exception as e:
            self.logger.error(f"Failed to release job {job.id}: {str(e)}")
            raise

    def bury(self, job_or_job_id: any, priority: int = 1000):
        """Bury a job. Accepts job object or job ID."""
        self._ensure_connection()
        job_id_to_bury = None
        try:
            job_id_to_bury = job_or_job_id.id if hasattr(job_or_job_id, 'id') and hasattr(job_or_job_id, 'body') else job_or_job_id
            if not isinstance(job_id_to_bury, (int, str)): # Basic check for ID type
                raise ValueError(f"Invalid job_id_to_bury type: {type(job_id_to_bury)}")
            self.connection.bury(job_id_to_bury, priority=priority)
            self.logger.debug(f"Buried job {job_id_to_bury} with priority {priority}")
        except Exception as e:
            self.logger.error(f"Failed to bury job {job_id_to_bury if job_id_to_bury is not None else 'unknown'}: {str(e)}")
            raise

    def get_job_stats(self, job_obj_or_id) -> dict | None:
        """Get statistics for a specific job. Accepts job object or job ID."""
        self._ensure_connection()
        job_id_to_stat = None
        try:
            job_id_to_stat = job_obj_or_id.id if hasattr(job_obj_or_id, 'id') and hasattr(job_obj_or_id, 'body') else job_obj_or_id
            if not isinstance(job_id_to_stat, (int, str)): # Basic check for ID type
                raise ValueError(f"Invalid job_id_to_stat type: {type(job_id_to_stat)}")
            stats = self.connection.stats_job(job_id_to_stat)
            self.logger.debug(f"Got stats for job {job_id_to_stat}: {stats}")
            return stats
        except greenstalk.NotFoundError:
            self.logger.warning(f"Job {job_id_to_stat} not found when trying to get stats.")
            return None # Or specific error
        except Exception as e:
            self.logger.error(f"Failed to get stats for job {job_id_to_stat if job_id_to_stat is not None else 'unknown'}: {str(e)}")
            return None # Or re-raise

    def tubes(self):
        """List all tubes"""
        self._ensure_connection()
        try:
            tube_list = self.connection.tubes()
            self.logger.debug(f"Tubes available: {tube_list}")
            return tube_list
        except Exception as e:
            self.logger.error(f"Failed to list tubes: {str(e)}")
            self.connection = None
            raise

    def stats_tube(self, tube):
        """Get stats for a tube"""
        self._ensure_connection()
        try:
            stats = self.connection.stats_tube(tube)
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get stats for tube {tube}: {str(e)}")
            raise

    def stats(self):
        """Get server stats"""
        self._ensure_connection()
        try:
            return self.connection.stats()
        except Exception as e:
            self.logger.error(f"Failed to get server stats: {str(e)}")
            raise

    def close(self):
        """Close the connection"""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Closed beanstalkd connection")
            except Exception as e:
                self.logger.error(f"Error closing beanstalkd connection: {str(e)}")
            finally:
                self.connection = None
                self.current_tube = None

    def peek_ready(self, tube):
        self._ensure_connection()
        self.connection.use(tube)
        try:
            return self.connection.peek_ready()
        except Exception as e:
            self.logger.error(f"Failed to peek ready job in tube {tube}: {str(e)}")
            return None

    def peek_delayed(self, tube):
        self._ensure_connection()
        self.connection.use(tube)
        try:
            return self.connection.peek_delayed()
        except Exception as e:
            self.logger.error(f"Failed to peek delayed job in tube {tube}: {str(e)}")
            return None

    def peek_buried(self, tube):
        self._ensure_connection()
        self.connection.use(tube)
        try:
            return self.connection.peek_buried()
        except Exception as e:
            self.logger.error(f"Failed to peek buried job in tube {tube}: {str(e)}")
            return None