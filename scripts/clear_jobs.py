#!/usr/bin/env python
import os
import sys
import logging
from datetime import datetime
import argparse

from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT, LOG_DIR, BEANSTALKD_CRAWL_TUBE

def setup_logging():
    """Set up logging"""
    log_filename = f"clear_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(LOG_DIR, log_filename)

    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode='a', encoding='utf-8')
        ]
    )
    return logging.getLogger("clear_jobs")

def list_jobs(queue_manager):
    """List the next ready job in the specified tube (Beanstalkd limitation)"""
    logger = logging.getLogger("clear_jobs")
    logger.info("Listing the next ready job in the 'crawl_jobs' tube...")
    queue_manager.client.use_tube(BEANSTALKD_CRAWL_TUBE)
    job = queue_manager.client.peek_ready(BEANSTALKD_CRAWL_TUBE)
    if job:
        job_data = queue_manager.serializer.deserialize_job(job.body)
        logger.info(f"Job ID: {job.id}, Data: {job_data}")
    else:
        logger.info("No ready jobs found in the 'crawl_jobs' tube.")

def clean_jobs(queue_manager):
    """Clean/delete all jobs from the specified tube"""
    logger = logging.getLogger("clear_jobs")
    logger.info("Cleaning all jobs from the 'crawl_jobs' tube...")
    queue_manager.client.use_tube(BEANSTALKD_CRAWL_TUBE)
    queue_manager.client.watch_tube(BEANSTALKD_CRAWL_TUBE)

    while True:
        job = queue_manager.client.reserve(timeout=0)
        if not job:
            break
        job.delete()
        logger.info(f"Deleted job {job.jid} from Beanstalkd")

def clean_job_by_id(queue_manager, job_id):
    """Clean/delete a specific job by ID"""
    logger = logging.getLogger("clear_jobs")
    logger.info(f"Cleaning job with ID: {job_id} from the 'crawl_jobs' tube...")
    queue_manager.client.use_tube(BEANSTALKD_CRAWL_TUBE)
    queue_manager.client.watch_tube(BEANSTALKD_CRAWL_TUBE)

    job = queue_manager.client.reserve(timeout=0)
    if job and job.jid == job_id:
        job.delete()
        logger.info(f"Deleted job {job.jid} from Beanstalkd")
    else:
        logger.warning(f"Job ID {job_id} not found in the queue.")

def clean_queue(queue_manager):
    """Clean/delete all jobs from all tubes"""
    logger = logging.getLogger("clear_jobs")
    logger.info("Cleaning all jobs from all tubes...")
    tubes = queue_manager.client.tubes()

    for tube in tubes:
        logger.info(f"Cleaning jobs from tube: {tube}")
        queue_manager.client.use_tube(tube)
        queue_manager.client.watch_tube(tube)

        while True:
            job = queue_manager.client.reserve(timeout=0)
            if not job:
                break
            job.delete()
            logger.info(f"Deleted job {job.jid} from tube {tube}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Manage jobs in Beanstalkd')
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')
    parser.add_argument('--list-jobs', action='store_true', help='List all jobs')
    parser.add_argument('--clean-jobs', action='store_true', help='Clean/delete all jobs')
    parser.add_argument('--clean-job-id', type=str, help='Clean/delete the job with the given ID')
    parser.add_argument('--clean-queue', action='store_true', help='Clean/delete all jobs from all queues')

    args = parser.parse_args()
    logger = setup_logging()

    try:
        queue_manager = QueueManager(host=args.queue_host, port=args.queue_port)

        if args.list_jobs:
            list_jobs(queue_manager)
        elif args.clean_jobs:
            clean_jobs(queue_manager)
        elif args.clean_job_id:
            clean_job_by_id(queue_manager, args.clean_job_id)
        elif args.clean_queue:
            clean_queue(queue_manager)
        else:
            logger.warning("No valid command provided. Use --list-jobs, --clean-jobs, --clean-job-id <job_id>, or --clean-queue.")

        queue_manager.close()
        return 0
    except Exception as e:
        logger.error(f"Error managing jobs: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())