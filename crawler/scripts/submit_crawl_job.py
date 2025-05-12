#!/usr/bin/env python
import os
import sys
import argparse
from datetime import datetime
import logging
from typing import Any, Dict
from urllib.parse import urlparse
import uuid

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR, DEFAULT_MAX_PAGES, DEFAULT_SINGLE_URL, DEFAULT_USE_SITEMAP, MONGO_CRAWL_JOB_COLLECTION, BEANSTALKD_TTR
from lib.storage.mongodb_client import MongoDBClient

def setup_logging(domain):
    log_filename = f"submit_{domain}.log"
    log_path = os.path.join(LOG_DIR, log_filename)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode='a', encoding='utf-8')
        ]
    )

def extract_domain_from_url(url):
    """Extract domain from URL if URL is provided"""
    if url:
        try:
            parsed_url = urlparse(url)
            return parsed_url.netloc
        except Exception:
            return None
    return None

def get_job_params(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get job parameters with defaults for missing values

    Args:
        job_data (Dict[str, Any]): Job data

    Returns:
        Dict[str, Any]: Job data with default values
    """
    if 'max_pages' not in job_data or job_data['max_pages'] is None:
        job_data['max_pages'] = int(DEFAULT_MAX_PAGES)

    if 'single_url' not in job_data or job_data['single_url'] is None:
        job_data['single_url'] = str(DEFAULT_SINGLE_URL).lower() in ('true', 'yes', '1')

    if 'use_sitemap' not in job_data or job_data['use_sitemap'] is None:
        job_data['use_sitemap'] = str(DEFAULT_USE_SITEMAP).lower() in ('true', 'yes', '1')

    return job_data

def submit_crawl_job(args):
    """
    Submit a crawl job to the Beanstalkd queue and update/insert MongoDB record.
    Args:
        args: Command line arguments
    Returns:
        str: Beanstalkd Job ID
    """
    queue_host = args.queue_host
    queue_port = int(args.queue_port)
    logger = logging.getLogger(__name__)

    # Initialize MongoDB client early
    mongodb_client = MongoDBClient()
    final_crawl_id = None
    is_new_task = True

    # Determine domain for logging and query
    current_domain = args.domain
    if args.url and not args.domain:
        extracted_domain = extract_domain_from_url(args.url)
        if extracted_domain:
            current_domain = extracted_domain
            logger.info(f"Extracted domain from URL: {current_domain}")
        else:
            logger.error("URL provided but domain could not be extracted and was not specified.")
            mongodb_client.close()
            return None

    # Try to find an existing, active job for this domain (and URL if provided)
    existing_job_query = {'job_data.domain': current_domain}
    if args.url:
        existing_job_query['job_data.url'] = args.url
    existing_job_query['crawl_status'] = {'$nin': ['completed', 'failed']}

    existing_task_doc = mongodb_client.find_one(MONGO_CRAWL_JOB_COLLECTION, existing_job_query)

    if existing_task_doc:
        final_crawl_id = existing_task_doc['crawl_id']
        is_new_task = False
        logger.info(f"Found existing active task for domain {current_domain} with crawl_id={final_crawl_id}. This submission will update and re-queue this task.")
    else:
        final_crawl_id = str(uuid.uuid4())
        is_new_task = True
        logger.info(f"No existing active task for domain {current_domain}. Creating new task with crawl_id={final_crawl_id}.")

    # Prepare job_data for Beanstalkd (this will be stored in MongoDB as well)
    job_data_for_beanstalkd = {
        'job_type': 'crawl',
        'submitted_at': datetime.utcnow().isoformat(),
        'crawl_id': final_crawl_id
    }
    if current_domain:
        job_data_for_beanstalkd['domain'] = current_domain
    if args.url:
        job_data_for_beanstalkd['url'] = args.url
        if args.single_url is None:
            job_data_for_beanstalkd['single_url'] = True
            logger.info("URL parameter provided - setting single_url=True by default for this submission.")

    if args.max_pages is not None:
        job_data_for_beanstalkd['max_pages'] = args.max_pages
    if args.single_url is not None:
        job_data_for_beanstalkd['single_url'] = args.single_url
    if args.use_sitemap is not None:
        job_data_for_beanstalkd['use_sitemap'] = args.use_sitemap

    # Apply environment defaults to the job_data that will be enqueued
    job_data_for_beanstalkd = get_job_params(job_data_for_beanstalkd)

    logger.info(f"Preparing to enqueue crawl job for {current_domain} with crawl_id {final_crawl_id}")

    queue_manager = QueueManager(host=queue_host, port=queue_port)
    beanstalkd_job_id = None
    try:
        beanstalkd_job_id = queue_manager.enqueue_job(
            job_data=job_data_for_beanstalkd,
            tube=args.tube,
            priority=args.priority,
            ttr=getattr(args, 'ttr', BEANSTALKD_TTR)
        )
    except Exception as e_enqueue:
        logger.error(f"Failed to enqueue job for {current_domain} (crawl_id={final_crawl_id}): {e_enqueue}")
        mongodb_client.close()
        queue_manager.close()
        raise

    # Prepare the MongoDB document payload
    mongo_doc_payload = {
        'job_id': beanstalkd_job_id,
        'job_data': job_data_for_beanstalkd,
        'crawl_status': 'fresh',
        'updated_at': datetime.utcnow()
    }

    try:
        if is_new_task:
            mongo_doc_payload['crawl_id'] = final_crawl_id
            mongo_doc_payload['created_at'] = mongo_doc_payload['updated_at']

            mongodb_client.insert_one(MONGO_CRAWL_JOB_COLLECTION, mongo_doc_payload)
            logger.info(f"Inserted new task in MongoDB for domain {current_domain} (crawl_id={final_crawl_id}, beanstalkd_job_id={beanstalkd_job_id})")
        else:
            update_query = {'_id': existing_task_doc['_id']}
            mongodb_client.update_one(MONGO_CRAWL_JOB_COLLECTION, update_query, {'$set': mongo_doc_payload})
            logger.info(f"Updated existing task in MongoDB for domain {current_domain} (crawl_id={final_crawl_id}, new beanstalkd_job_id={beanstalkd_job_id})")
    except Exception as e_mongo:
        logger.error(f"MongoDB operation failed for {current_domain} (crawl_id={final_crawl_id}, beanstalkd_job_id={beanstalkd_job_id}): {e_mongo}")
        mongodb_client.close()
        queue_manager.close()
        raise

    mongodb_client.close()
    queue_manager.close()

    logger.info(f"Successfully submitted job for domain: {current_domain}")
    print(f"Submitting job with the following parameters for crawl_id: {final_crawl_id}, beanstalkd_job_id: {beanstalkd_job_id}")
    for key, value in job_data_for_beanstalkd.items():
        if key not in ['submitted_at', '_job']:
            print(f"  {key}: {value}")

    return beanstalkd_job_id

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Submit a crawl job to the queue')
    parser.add_argument('--domain', help='Domain to crawl')
    parser.add_argument('--url', help='URL to crawl (for single URL mode, implies domain)')
    parser.add_argument('--max-pages', type=int, default=None, help=f'Maximum pages to crawl (default: {DEFAULT_MAX_PAGES} from .env)')
    parser.add_argument('--single-url', action=argparse.BooleanOptionalAction, default=None, help='Crawl a single URL only (auto-set if --url is main identifier)')

    sitemap_group = parser.add_mutually_exclusive_group()
    sitemap_group.add_argument('--use-sitemap', dest='use_sitemap', action='store_true', default=None, help='Use sitemap for URL discovery')
    sitemap_group.add_argument('--no-sitemap', dest='use_sitemap', action='store_false', help='Do not use sitemap for URL discovery')

    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')
    parser.add_argument('--tube', default=MONGO_CRAWL_JOB_COLLECTION, help='Beanstalkd tube (default: crawl_jobs)')
    parser.add_argument('--priority', default='normal', help='Job priority (high, normal, low)')
    parser.add_argument('--ttr', type=int, default=None, help=f'Beanstalkd Time-To-Run in seconds (default: {BEANSTALKD_TTR} from .env)')

    args = parser.parse_args()

    domain_for_log = args.domain
    if args.url and not args.domain:
        extracted_domain = extract_domain_from_url(args.url)
        if extracted_domain:
            domain_for_log = extracted_domain
        else:
            print("Error: --url provided but could not extract domain, and --domain not specified.")
            sys.exit(1)
    elif not args.domain and not args.url:
        parser.error("Either --domain or --url is required.")

    if args.ttr is None:
        args.ttr = BEANSTALKD_TTR

    setup_logging(domain_for_log if domain_for_log else "unknown_submission")

    try:
        job_id_result = submit_crawl_job(args)
        if job_id_result:
            print(f"\nJob submission process completed. Beanstalkd Job ID: {job_id_result}")
            sys.exit(0)
        else:
            print("\nJob submission failed (no Beanstalkd Job ID returned). Check logs.")
            sys.exit(1)
    except Exception as e:
        print(f"Error during job submission process: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()