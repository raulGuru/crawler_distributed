#!/usr/bin/env python
import os
import sys
import argparse
import uuid
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


def setup_logging(log_name="bulk_domain_ingestor"):
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

def bulk_ingest_domains(args):

    logger = setup_logging()
    logger.info("Starting bulk domain ingestion process...")

    mongodb_client = None
    queue_manager = None

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
            return

        logger.info(f"Calculated capacity to submit {num_to_submit} new domains.")

        # 3. Fetch domains from domains_crawl collection
        source_domains_query = {'status': args.source_status}
        domain_docs = list(mongodb_client.find(args.domains_collection, source_domains_query, limit=num_to_submit))

        if not domain_docs:
            logger.info(f"No domains found in '{args.domains_collection}' with status '{args.source_status}'. Exiting.")
            return

        logger.info(f"Fetched {len(domain_docs)} domains to process.")
        submitted_count = 0

        for domain_doc in domain_docs:
            domain_id_in_source = domain_doc['_id']
            domain_name = domain_doc.get('domain')
            url = None
            if 'url' in domain_doc:
                url = domain_doc['url']
            custom_params_from_doc = domain_doc.get('custom_params', {}) # Expects a dict
            if 'cycle_id' in domain_doc:
                custom_params_from_doc['cycle_id'] = domain_doc['cycle_id']
            if 'projects_id' in domain_doc:
                custom_params_from_doc['projects_id'] = domain_doc['projects_id']

            if not domain_name:
                logger.warning(f"Skipping domain with _id {domain_id_in_source} due to missing 'domain_name'.")
                continue

            logger.info(f"Processing domain: {domain_name} (ID: {domain_id_in_source})")

            # Lock domain in source collection
            lock_update = mongodb_client.update_one(
                args.domains_collection,
                {'_id': domain_id_in_source, 'status': args.source_status}, # Ensure status hasn't changed
                {'$set': {'status': args.pending_status, 'last_attempted_at': datetime.utcnow()}}
            )
            if lock_update['modified_count'] == 0:
                logger.warning(f"Failed to lock domain {domain_name} (ID: {domain_id_in_source}). It might have been picked up by another process. Skipping.")
                continue

            beanstalkd_job_id = None
            crawl_id = str(uuid.uuid4())

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
                }
                # Overlay custom parameters from the domain document
                if isinstance(custom_params_from_doc, dict):
                    job_data_for_beanstalkd.update(custom_params_from_doc)

                # Apply general defaults for any missing critical params
                final_job_data = get_job_params(job_data_for_beanstalkd)

                logger.debug(f"Final job data for {domain_name}: {final_job_data}")

                # Enqueue job to Beanstalkd
                beanstalkd_job_id = queue_manager.enqueue_job(
                    job_data=final_job_data,
                    tube=QUEUE_CRAWL_TUBE,
                    priority='high', # Using arg for priority
                    ttr=QUEUE_TTR
                )
                logger.info(f"Enqueued job for {domain_name} to Beanstalkd. Job ID: {beanstalkd_job_id}, Crawl ID: {crawl_id}")

                # Prepare MongoDB document for crawl_jobs collection
                mongo_doc_payload = {
                    **final_job_data, # Includes crawl_id, domain, all params
                    'job_id': beanstalkd_job_id, # Beanstalkd job ID
                    'crawl_status': 'fresh',
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
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
                        'crawl_id_ref': crawl_id # Store reference to the crawl_id
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
                    {'_id': domain_id_in_source, 'status': args.pending_status}, # only if still pending
                    {'$set': {'status': args.source_status, 'error_message': str(e)[:500]}} # Revert with error
                )
                logger.info(f"Reverted status for domain {domain_name} to '{args.source_status}' due to error.")

                # If job was enqueued but DB failed, it's an orphan. Critical log.
                if beanstalkd_job_id and not insert_result_id:
                     logger.critical(f"CRITICAL: Job {beanstalkd_job_id} for {domain_name} (crawl_id {crawl_id}) enqueued but failed to save to MongoDB. Manual intervention may be needed.")
                # Note: Deleting from Beanstalkd by job_id is not directly supported by basic client.
                # Advanced Beanstalkd management might be needed for full rollback.

        logger.info(f"Finished processing batch. Submitted {submitted_count} out of {len(domain_docs)} fetched domains.")

    except Exception as e:
        logger.error(f"An unhandled error occurred in the bulk ingestion process: {str(e)}")
        LoggingUtils.log_exception(logger, e, "Unhandled error in bulk_ingest_domains")
    finally:
        if mongodb_client:
            mongodb_client.close()
            logger.info("MongoDB connection closed.")
        if queue_manager:
            queue_manager.close()
            logger.info("Beanstalkd connection closed.")
        logger.info("Bulk domain ingestion process ended.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk ingest domains for crawling.")

    # Beanstalkd connection args
    parser.add_argument("--queue-host", default=QUEUE_HOST, help="Beanstalkd host.")
    parser.add_argument("--queue-port", type=int, default=QUEUE_PORT, help="Beanstalkd port.")

    # Ingestion logic args
    parser.add_argument("--domains-collection", default="domains_crawl", help="MongoDB collection for sourcing domains.")
    parser.add_argument("--source-status", default="new", help="Status of domains to pick from domains_collection.")
    parser.add_argument("--pending-status", default="pending_submission", help="Temporary status while processing a domain.")
    parser.add_argument("--submitted-status", default="submitted_to_crawler", help="Final status in domains_collection after successful submission.")
    parser.add_argument("--buffer-factor", type=float, default=1.5, help="Multiplier for CRAWLER_INSTANCES to determine submission capacity buffer.")
    parser.add_argument("--limit", type=int, default=None, help="Hard limit on the number of domains to submit in one run (overrides calculated capacity if lower).")

    args = parser.parse_args()

    bulk_ingest_domains(args)