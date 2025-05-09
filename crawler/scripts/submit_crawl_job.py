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
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR, DEFAULT_MAX_PAGES, DEFAULT_SINGLE_URL, DEFAULT_USE_SITEMAP, MONGO_CRAWL_JOB_COLLECTION
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
    Submit a crawl job to the Beanstalkd queue
    Args:
        args: Command line arguments
    Returns:
        str: Job ID
    """
    # Initialize queue manager
    queue_host = args.queue_host
    queue_port = int(args.queue_port)
    queue_manager = QueueManager(host=queue_host, port=queue_port)

    # Create job data with only defined parameters
    job_data = {
        'job_type': 'crawl',
        'submitted_at': datetime.utcnow().isoformat()
    }

    # Extract domain from URL if URL provided but domain not
    if args.url and not args.domain:
        extracted_domain = extract_domain_from_url(args.url)
        if extracted_domain:
            print(f"Extracted domain from URL: {extracted_domain}")
            args.domain = extracted_domain

    # Add only provided parameters
    if args.domain:
        job_data['domain'] = args.domain

    if args.url:
        job_data['url'] = args.url
        if args.single_url is None:
            job_data['single_url'] = True
            print("URL parameter provided - setting single_url=True")

    if args.max_pages is not None:
        job_data['max_pages'] = args.max_pages

    if args.single_url is not None:
        job_data['single_url'] = args.single_url

    if args.use_sitemap is not None:
        job_data['use_sitemap'] = args.use_sitemap

    # Ensure a unique crawl_id is present
    if 'crawl_id' not in job_data or not job_data['crawl_id']:
        job_data['crawl_id'] = str(uuid.uuid4())
    crawl_id = job_data['crawl_id']

    # Fill in missing parameters with defaults from .env
    job_data = get_job_params(job_data)

    # Enqueue the full job data first to get job_id
    job_id = queue_manager.enqueue_job(
        job_data=job_data,
        tube=args.tube,
        priority=args.priority
    )

    # Insert or update job in MongoDB with crawl_status 'fresh' and job_id
    mongodb_client = MongoDBClient()
    # Check for existing job for the same domain (and url if present) that is not completed/failed
    query = {'job_data.domain': job_data.get('domain')}
    if job_data.get('url'):
        query['job_data.url'] = job_data.get('url')
    query['crawl_status'] = {'$nin': ['completed', 'failed']}
    existing_job = mongodb_client.find_one(MONGO_CRAWL_JOB_COLLECTION, query)
    job_doc = {
        'crawl_id': crawl_id,
        'job_id': job_id,
        'job_data': job_data,
        'crawl_status': 'fresh',
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    if existing_job:
        # Update the existing job with new parameters and set crawl_status to 'fresh'
        mongodb_client.update_one(MONGO_CRAWL_JOB_COLLECTION, {'_id': existing_job['_id']}, {'$set': job_doc}, upsert=False)
        print(f"Updated existing job for domain {job_data.get('domain')} (crawl_id={existing_job['crawl_id']})")
        crawl_id = existing_job['crawl_id']
    else:
        mongodb_client.insert_one(MONGO_CRAWL_JOB_COLLECTION, job_doc)
        print(f"Inserted new job for domain {job_data.get('domain')} (crawl_id={crawl_id})")
    mongodb_client.close()

    print(f"Submitting job with the following parameters:")
    for key, value in job_data.items():
        if key != 'submitted_at':
            print(f"  {key}: {value}")

    queue_manager.close()
    return job_id

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Submit a crawl job to the queue')
    parser.add_argument('--domain', help='Domain to crawl')
    parser.add_argument('--url', help='URL to crawl (for single URL mode)')
    parser.add_argument('--max-pages', type=int, default=None, help=f'Maximum pages to crawl (default: {DEFAULT_MAX_PAGES})')
    parser.add_argument('--single-url', action='store_true', default=None, help='Crawl a single URL only (implied when --url is provided)')
    sitemap_group = parser.add_mutually_exclusive_group()
    sitemap_group.add_argument('--use-sitemap', dest='use_sitemap', action='store_true', help='Use sitemap for URL discovery')
    sitemap_group.add_argument('--no-sitemap', dest='use_sitemap', action='store_false', help='Do not use sitemap for URL discovery')
    parser.set_defaults(use_sitemap=None)
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')
    parser.add_argument('--tube', default=MONGO_CRAWL_JOB_COLLECTION, help='Beanstalkd tube')
    parser.add_argument('--priority', default='normal', help='Job priority (high, normal, low)')
    args = parser.parse_args()
    if not args.domain and not args.url:
        parser.error("Either --domain or --url is required")
    if args.url and not args.domain:
        extracted_domain = extract_domain_from_url(args.url)
        if extracted_domain:
            args.domain = extracted_domain
        else:
            parser.error("Could not extract domain from URL. Please provide --domain parameter.")
    setup_logging(args.domain if args.domain else "unknown")
    try:
        job_id = submit_crawl_job(args)
        if job_id:
            print(f"\nJob submitted successfully with ID: {job_id}")
            return 0
        else:
            return 1
    except Exception as e:
        print(f"Error submitting job: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())