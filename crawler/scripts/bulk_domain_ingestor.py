#!/usr/bin/env python
import os
import sys
import argparse
import uuid
import time
import signal
from datetime import datetime

# Add project root to sys.path to allow importing project modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.utils.logging_utils import LoggingUtils
from config.base_settings import (
    QUEUE_HOST, QUEUE_PORT, MONGO_CRAWL_JOB_COLLECTION, QUEUE_CRAWL_TUBE,
    DEFAULT_MAX_PAGES, DEFAULT_SINGLE_URL, DEFAULT_USE_SITEMAP, CRAWLER_INSTANCES, QUEUE_TTR,
    LOG_DIR,
    LOG_LEVEL
)


class BulkDomainScheduler:
    """
    Scheduler for bulk domain ingestion that runs periodically.

    This class manages the lifecycle of periodic domain ingestion,
    including signal handling, graceful shutdown, and error recovery.
    """

    def __init__(self, interval_seconds: int, args: argparse.Namespace):
        """
        Initialize the scheduler.

        Args:
            interval_seconds: Interval between ingestion runs in seconds
            args: Command line arguments for ingestion configuration
        """
        self.interval_seconds = interval_seconds
        self.args = args
        self.running = False
        self.shutdown_requested = False
        self.logger = self._setup_logging()

        # Statistics tracking
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'total_domains_submitted': 0,
            'start_time': None,
            'last_run_time': None,
        }

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_logging(self):
        """Set up logging for the scheduler."""
        # Create a specific log directory for the bulk ingestor if it doesn't exist
        ingestor_log_dir = os.path.join(LOG_DIR, "bulk_ingestor_logs")
        if not os.path.exists(ingestor_log_dir):
            os.makedirs(ingestor_log_dir, exist_ok=True)

        log_path = os.path.join(ingestor_log_dir, "bulk_domain_scheduler.log")
        logger = LoggingUtils.setup_logger(
            name="bulk_domain_scheduler",
            log_file=log_path,
            level=LOG_LEVEL,
            console=True,
            json_format=False,
        )
        return logger

    def _handle_signal(self, signum: int, frame):
        """Handle termination signals gracefully."""
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        self.logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")
        self.shutdown_requested = True

    def start(self) -> bool:
        """
        Start the periodic domain ingestion scheduler.

        Returns:
            bool: True if scheduler started successfully, False otherwise
        """
        self.logger.info(f"Starting bulk domain ingestion scheduler with {self.interval_seconds}s interval")
        self.logger.info(f"Scheduler configuration: domains_collection={self.args.domains_collection}, "
                        f"source_status={self.args.source_status}, "
                        f"buffer_factor={self.args.buffer_factor}")

        self.running = True
        self.stats['start_time'] = datetime.utcnow()

        try:
            # Main scheduler loop
            while self.running and not self.shutdown_requested:
                try:
                    # Record run start time
                    run_start_time = time.time()
                    self.stats['last_run_time'] = datetime.utcnow()
                    self.stats['total_runs'] += 1

                    self.logger.info(f"Starting ingestion run #{self.stats['total_runs']}")

                    # Execute domain ingestion
                    domains_submitted = self._execute_ingestion_cycle()

                    # Update statistics
                    if domains_submitted is not None:
                        self.stats['successful_runs'] += 1
                        self.stats['total_domains_submitted'] += domains_submitted
                        run_duration = time.time() - run_start_time
                        self.logger.info(f"Completed ingestion run #{self.stats['total_runs']} successfully. "
                                       f"Submitted {domains_submitted} domains in {run_duration:.2f}s")
                    else:
                        self.stats['failed_runs'] += 1
                        self.logger.error(f"Ingestion run #{self.stats['total_runs']} failed")

                    # Log periodic statistics
                    if self.stats['total_runs'] % 10 == 0:
                        self._log_statistics()

                    # Wait for next cycle with interruptible sleep
                    if not self.shutdown_requested:
                        self._interruptible_sleep(self.interval_seconds)

                except Exception as e:
                    self.stats['failed_runs'] += 1
                    self.logger.error(f"Unexpected error in ingestion run #{self.stats['total_runs']}: {str(e)}")
                    LoggingUtils.log_exception(self.logger, e, f"Error in ingestion run #{self.stats['total_runs']}")

                    # Sleep before retrying, but with shorter interval on error
                    error_sleep_time = min(60, self.interval_seconds)  # Cap at 1 minute
                    self.logger.info(f"Sleeping {error_sleep_time}s before retry due to error")
                    self._interruptible_sleep(error_sleep_time)

        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
            self.shutdown_requested = True
        finally:
            self._shutdown()

        return True

    def _execute_ingestion_cycle(self) -> int:
        """
        Execute a single ingestion cycle.

        Returns:
            int: Number of domains submitted, or None if failed
        """
        return bulk_ingest_domains_single_run(self.args, self.logger)

    def _interruptible_sleep(self, sleep_seconds: int):
        """
        Sleep for the specified duration with the ability to be interrupted.

        Args:
            sleep_seconds: Number of seconds to sleep
        """
        for _ in range(sleep_seconds):
            if self.shutdown_requested:
                break
            time.sleep(1)

    def _log_statistics(self):
        """Log periodic statistics about the scheduler performance."""
        uptime = datetime.utcnow() - self.stats['start_time']
        success_rate = (self.stats['successful_runs'] / self.stats['total_runs'] * 100) if self.stats['total_runs'] > 0 else 0
        avg_domains_per_run = (self.stats['total_domains_submitted'] / self.stats['successful_runs']) if self.stats['successful_runs'] > 0 else 0

        self.logger.info("=== Scheduler Statistics ===")
        self.logger.info(f"Uptime: {uptime}")
        self.logger.info(f"Total runs: {self.stats['total_runs']}")
        self.logger.info(f"Successful runs: {self.stats['successful_runs']} ({success_rate:.1f}%)")
        self.logger.info(f"Failed runs: {self.stats['failed_runs']}")
        self.logger.info(f"Total domains submitted: {self.stats['total_domains_submitted']}")
        self.logger.info(f"Average domains per successful run: {avg_domains_per_run:.1f}")
        self.logger.info("============================")

    def _shutdown(self):
        """Perform cleanup and shutdown procedures."""
        self.running = False
        self.logger.info("Bulk domain ingestion scheduler shutting down...")

        # Log final statistics
        self._log_statistics()

        # Calculate final uptime
        if self.stats['start_time']:
            uptime = datetime.utcnow() - self.stats['start_time']
            self.logger.info(f"Scheduler ran for {uptime} total")

        self.logger.info("Bulk domain ingestion scheduler stopped")


def setup_single_run_logging(log_name="bulk_domain_ingestor"):
    """Set up logging for single run mode (backward compatibility)."""
    # Create a specific log directory for the bulk ingestor if it doesn't exist
    ingestor_log_dir = os.path.join(LOG_DIR, "bulk_ingestor_logs")
    if not os.path.exists(ingestor_log_dir):
        os.makedirs(ingestor_log_dir, exist_ok=True)

    log_path = os.path.join(ingestor_log_dir, f"{log_name}.log")
    logger = LoggingUtils.setup_logger(
        name=log_name,
        log_file=log_path,
        level=LOG_LEVEL,
        console=True,
        json_format=False,
    )
    return logger


def get_job_params(job_data: dict) -> dict:
    """
    Ensure job parameters have defaults for missing values, similar to submit_crawl_job.py.
    """
    if 'max_pages' not in job_data or job_data['max_pages'] is None:
        job_data['max_pages'] = int(DEFAULT_MAX_PAGES)

    # For bulk ingestion, single_url is typically False unless specified in custom_params
    if 'single_url' not in job_data or job_data['single_url'] is None:
        job_data['single_url'] = str(DEFAULT_SINGLE_URL).lower() in ('true', 'yes', '1')

    if 'use_sitemap' not in job_data or job_data['use_sitemap'] is None:
        job_data['use_sitemap'] = str(DEFAULT_USE_SITEMAP).lower() in ('true', 'yes', '1')

    # Ensure boolean values are actual booleans if they are string representations
    if isinstance(job_data.get('single_url'), str):
        job_data['single_url'] = job_data['single_url'].lower() in ('true', 'yes', '1')
    if isinstance(job_data.get('use_sitemap'), str):
        job_data['use_sitemap'] = job_data['use_sitemap'].lower() in ('true', 'yes', '1')

    if 'url' in job_data and job_data['url'] is not None:
        job_data['single_url'] = True
        job_data['max_pages'] = 1
        job_data['use_sitemap'] = False

    return job_data


def bulk_ingest_domains_single_run(args, logger=None) -> int:
    """
    Execute a single run of domain ingestion.

    Args:
        args: Command line arguments
        logger: Optional logger (will create one if not provided)

    Returns:
        int: Number of domains submitted, or None if failed
    """
    if logger is None:
        logger = setup_single_run_logging()

    logger.info("Starting bulk domain ingestion process...")

    mongodb_client = None
    queue_manager = None
    submitted_count = 0

    try:
        mongodb_client = MongoDBClient(logger=logger)

        queue_manager = QueueManager(host=QUEUE_HOST, port=QUEUE_PORT, logger=logger)

        # 1. Check Beanstalkd queue stats
        tube_stats = queue_manager.stats_tube(QUEUE_CRAWL_TUBE)
        if not tube_stats:
            logger.warning(f"Could not get stats for tube '{QUEUE_CRAWL_TUBE}'. Assuming empty or using defaults.")
            jobs_ready = 0
            jobs_reserved = 0
        else:
            jobs_ready = tube_stats.get('current-jobs-ready', 0)
            jobs_reserved = tube_stats.get('current-jobs-reserved', 0)

        logger.info(f"Beanstalkd tube '{QUEUE_CRAWL_TUBE}' stats: {jobs_ready} jobs ready, {jobs_reserved} jobs reserved.")

        # 2. Calculate number of domains to submit
        # Using CRAWLER_INSTANCES from imported base_settings
        num_to_submit_calculated = int((CRAWLER_INSTANCES * args.buffer_factor) - (jobs_ready + jobs_reserved))

        if args.limit is not None and args.limit < num_to_submit_calculated:
            logger.info(f"Limiting submission to {args.limit} domains due to --limit flag (calculated: {num_to_submit_calculated}).")
            num_to_submit = args.limit
        else:
            num_to_submit = num_to_submit_calculated

        if num_to_submit <= 0:
            logger.info(f"No new domains needed at this time. Calculated capacity: {num_to_submit}. Exiting.")
            return 0

        logger.info(f"Calculated capacity to submit {num_to_submit} new domains.")

        # 3. Fetch domains from domains_crawl collection
        source_domains_query = {'status': args.source_status}
        domain_docs = list(mongodb_client.find(args.domains_collection, source_domains_query, limit=num_to_submit))

        if not domain_docs:
            logger.info(f"No domains found in '{args.domains_collection}' with status '{args.source_status}'. Exiting.")
            return 0

        logger.info(f"Fetched {len(domain_docs)} domains to process.")

        for domain_doc in domain_docs:
            domain_id_in_source = domain_doc['_id']
            domain_name = domain_doc.get('domain')
            url = domain_doc.get('url')


            if not domain_name:
                logger.warning(f"Skipping domain with _id {domain_id_in_source} due to missing 'domain_name'.")
                continue

            logger.info(f"Processing domain: {domain_name} (ID: {domain_id_in_source})")

            # Lock domain in source collection
            lock_update = mongodb_client.update_one(
                args.domains_collection,
                {'_id': domain_id_in_source, 'status': args.source_status},  # Ensure status hasn't changed
                {'$set': {'status': args.pending_status, 'last_attempted_at': datetime.utcnow()}}
            )
            if lock_update['modified_count'] == 0:
                logger.warning(f"Failed to lock domain {domain_name} (ID: {domain_id_in_source}). It might have been picked up by another process. Skipping.")
                continue

            beanstalkd_job_id = None
            crawl_id = str(uuid.uuid4())
            insert_result_id = None

            try:
                # Prepare job_data for Beanstalkd
                job_data_for_beanstalkd = {
                    'job_type': 'crawl',
                    'submitted_at': datetime.utcnow().isoformat(),
                    'crawl_id': crawl_id,
                    'domain': domain_name,
                    'url': url,
                    'max_pages': domain_doc.get('max_pages', DEFAULT_MAX_PAGES),
                    'single_url': domain_doc.get('single_url', DEFAULT_SINGLE_URL),
                    'use_sitemap': domain_doc.get('use_sitemap', DEFAULT_USE_SITEMAP),
                    'cycle_id': domain_doc.get('cycle_id', 0),
                    'project_id': domain_doc.get('project_id', None),
                }
                # Add any custom parameters to job_data
                custom_params_from_doc = domain_doc.get('custom_params', {})  # Expects a dict
                # Don't override existing parameters
                for key, value in custom_params_from_doc.items():
                    if key not in job_data_for_beanstalkd:
                        job_data_for_beanstalkd[key] = value
                    else:
                        logger.warning(f"Custom parameter '{key}' conflicts with standard parameter, ignoring custom value.")

                # Apply general defaults for any missing critical params
                final_job_data = get_job_params(job_data_for_beanstalkd)

                logger.debug(f"Final job data for {domain_name}: {final_job_data}")

                # Enqueue job to Beanstalkd
                beanstalkd_job_id = queue_manager.enqueue_job(
                    job_data=final_job_data,
                    tube=QUEUE_CRAWL_TUBE,
                    priority='high',  # Using high priority for bulk submission
                    ttr=QUEUE_TTR
                )
                logger.info(f"Enqueued job for {domain_name} to Beanstalkd. Job ID: {beanstalkd_job_id}, Crawl ID: {crawl_id}")

                # Prepare MongoDB document for crawl_jobs collection
                mongo_doc_payload = {
                    **final_job_data, # Includes crawl_id, domain, all params
                    'job_id': beanstalkd_job_id, # Beanstalkd job ID
                    'crawl_status': 'fresh',
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow(),
                    'project_id': final_job_data.get('project_id'),  # Required ObjectId
                    'cycle_id': int(final_job_data.get('cycle_id', 0)),  # Required int, default 0
                }

                insert_result_id = mongodb_client.insert_one(MONGO_CRAWL_JOB_COLLECTION, mongo_doc_payload)
                if not insert_result_id:
                    raise Exception(f"Failed to insert job into {MONGO_CRAWL_JOB_COLLECTION} for crawl_id {crawl_id}")

                logger.info(f"Inserted job into {MONGO_CRAWL_JOB_COLLECTION} for {domain_name} (Crawl ID: {crawl_id})")

                # Finalize status in domains_crawl collection
                mongodb_client.update_one(
                    args.domains_collection,
                    {'_id': domain_id_in_source},
                    {'$set': {
                        'status': args.submitted_status,
                        'last_submitted_at': datetime.utcnow(),
                        'crawl_id_ref': crawl_id  # Store reference to the crawl_id
                    }}
                )
                logger.info(f"Successfully submitted and recorded domain {domain_name}. Status updated to '{args.submitted_status}'.")
                submitted_count += 1

            except Exception as e:
                logger.error(f"Failed to process domain {domain_name} (ID: {domain_id_in_source}): {str(e)}")
                LoggingUtils.log_exception(logger, e, f"Error processing domain {domain_name}")

                # Rollback status in domains_crawl if it was locked
                mongodb_client.update_one(
                    args.domains_collection,
                    {'_id': domain_id_in_source, 'status': args.pending_status},  # only if still pending
                    {'$set': {'status': args.source_status, 'error_message': str(e)[:500]}}  # Revert with error
                )
                logger.info(f"Reverted status for domain {domain_name} to '{args.source_status}' due to error.")

                # If job was enqueued but DB failed, it's an orphan. Critical log.
                if beanstalkd_job_id and not insert_result_id:
                    logger.critical(f"CRITICAL: Job {beanstalkd_job_id} for {domain_name} (crawl_id {crawl_id}) enqueued but failed to save to MongoDB. Manual intervention may be needed.")

        logger.info(f"Finished processing batch. Submitted {submitted_count} out of {len(domain_docs)} fetched domains.")
        return submitted_count

    except Exception as e:
        logger.error(f"An unhandled error occurred in the bulk ingestion process: {str(e)}")
        LoggingUtils.log_exception(logger, e, "Unhandled error in bulk_ingest_domains_single_run")
        return None
    finally:
        if mongodb_client:
            mongodb_client.close()
            logger.debug("MongoDB connection closed.")
        if queue_manager:
            queue_manager.close()
            logger.debug("Beanstalkd connection closed.")


def bulk_ingest_domains(args):
    """
    Legacy function for backward compatibility.
    Now delegates to bulk_ingest_domains_single_run.
    """
    logger = setup_single_run_logging()
    return bulk_ingest_domains_single_run(args, logger)


def main():
    """Main entry point with enhanced argument parsing for scheduler mode."""
    parser = argparse.ArgumentParser(
        description="Bulk ingest domains for crawling with optional scheduler mode.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once (legacy mode)
  python bulk_domain_ingestor.py --run-once

  # Run every 30 seconds
  python bulk_domain_ingestor.py --interval-seconds 30

  # Run every 2 minutes
  python bulk_domain_ingestor.py --interval-minutes 2

  # Run every 5 minutes with custom buffer factor
  python bulk_domain_ingestor.py --interval-minutes 5 --buffer-factor 2.0
        """
    )

    # Scheduler mode arguments
    scheduler_group = parser.add_mutually_exclusive_group(required=False)
    scheduler_group.add_argument("--run-once", action="store_true",
                                help="Run once and exit (legacy mode, default if no interval specified)")
    scheduler_group.add_argument("--interval-seconds", type=int,
                                help="Run every N seconds in scheduler mode")
    scheduler_group.add_argument("--interval-minutes", type=int,
                                help="Run every N minutes in scheduler mode")

    # Beanstalkd connection args
    parser.add_argument("--queue-host", default=QUEUE_HOST, help="Beanstalkd host.")
    parser.add_argument("--queue-port", type=int, default=QUEUE_PORT, help="Beanstalkd port.")

    # Ingestion logic args
    parser.add_argument("--domains-collection", default="domains_crawl",
                       help="MongoDB collection for sourcing domains.")
    parser.add_argument("--source-status", default="new",
                       help="Status of domains to pick from domains_collection.")
    parser.add_argument("--pending-status", default="pending_submission",
                       help="Temporary status while processing a domain.")
    parser.add_argument("--submitted-status", default="submitted_to_crawler",
                       help="Final status in domains_collection after successful submission.")
    parser.add_argument("--buffer-factor", type=float, default=1.5,
                       help="Multiplier for CRAWLER_INSTANCES to determine submission capacity buffer.")
    parser.add_argument("--limit", type=int, default=None,
                       help="Hard limit on the number of domains to submit in one run (overrides calculated capacity if lower).")

    args = parser.parse_args()

    # Determine execution mode
    if args.interval_seconds:
        interval_seconds = args.interval_seconds
        print(f"Starting scheduler mode: running every {interval_seconds} seconds")
    elif args.interval_minutes:
        interval_seconds = args.interval_minutes * 60
        print(f"Starting scheduler mode: running every {args.interval_minutes} minutes ({interval_seconds} seconds)")
    else:
        # Default to run-once mode if no interval specified
        print("Running in single-run mode (use --interval-seconds or --interval-minutes for scheduler mode)")
        result = bulk_ingest_domains(args)
        if result is not None:
            print(f"Successfully submitted {result} domains")
            sys.exit(0)
        else:
            print("Domain ingestion failed")
            sys.exit(1)

    # Scheduler mode
    try:
        scheduler = BulkDomainScheduler(interval_seconds, args)
        success = scheduler.start()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Scheduler failed to start: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()