#!/usr/bin/env python
import os
import sys
import argparse
import logging
import json
from tabulate import tabulate
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('queue_status')

def print_tube_stats(queue_manager):
    """Print statistics for all tubes"""
    stats = queue_manager.get_stats()

    headers = ["Tube Name", "Ready", "Reserved", "Delayed", "Buried", "Total Jobs"]
    table_data = []

    for tube, tube_stats in stats['tubes'].items():
        table_data.append([
            tube,
            tube_stats['ready'],
            tube_stats['reserved'],
            tube_stats['delayed'],
            tube_stats['buried'],
            tube_stats['total_jobs']
        ])

    table_data.append([
        "TOTAL",
        stats['ready_jobs'],
        stats['reserved_jobs'],
        stats['delayed_jobs'],
        stats['buried_jobs'],
        stats['total_jobs']
    ])

    print("\n=== Beanstalkd Queue Status ===")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def list_jobs_by_state(queue_manager, tube, state='ready', limit=5, ids_only=False):
    """List jobs in a specific state in a tube"""
    queue_manager.client.use_tube(tube)
    queue_manager.client.watch_tube(tube)

    if not ids_only:
        print(f"\n=== {state.upper()} jobs in tube '{tube}' (up to {limit}) ===")

    jobs = []
    seen_ids = set()

    for _ in range(limit):
        try:
            job = None
            # Try to peek at job
            if state == 'ready':
                try:
                    job = queue_manager.client.connection.peek_ready()
                except:
                    break
            elif state == 'delayed':
                try:
                    job = queue_manager.client.connection.peek_delayed()
                except:
                    break
            elif state == 'buried':
                try:
                    job = queue_manager.client.connection.peek_buried()
                except:
                    break

            if not job or job.id in seen_ids:
                break

            seen_ids.add(job.id)
            jobs.append(job)

        except Exception as e:
            if not ids_only:
                print(f"Error accessing job: {str(e)}")
            break

    if ids_only:
        # Print only job IDs, one per line
        for job in jobs:
            print(job.id)
    elif jobs:
        headers = ["Job ID", "Priority", "Age", "TTR", "State", "Tube", "Data Preview"]
        table_data = []

        for job in jobs:
            stats = queue_manager.client.connection.stats_job(job.id)

            # Try to deserialize and get a preview of the job data
            try:
                job_data = queue_manager.serializer.deserialize_job(job.body)
                data_preview = str(job_data.get('domain', job_data.get('url', 'N/A')))
            except:
                data_preview = "Unable to deserialize"

            table_data.append([
                job.id,
                stats.get('pri', 'N/A'),
                f"{stats.get('age', 0)}s",
                f"{stats.get('ttr', 0)}s",
                state,
                stats.get('tube', 'N/A'),
                data_preview[:50] + ('...' if len(data_preview) > 50 else '')
            ])

        print(tabulate(table_data, headers=headers, tablefmt="grid"))
    elif not ids_only:
        print(f"No {state} jobs found in tube '{tube}'")

def get_job_details(queue_manager, job_id):
    """Get detailed information about a specific job"""
    try:
        # Try to find the job in different states and tubes
        job = None
        job_tube = None

        # Check all tubes
        for tube in queue_manager.client.tubes():
            queue_manager.client.use_tube(tube)
            queue_manager.client.watch_tube(tube)

            # Try different peek methods
            for peek_method in ['peek_ready', 'peek_delayed', 'peek_buried']:
                try:
                    peek_func = getattr(queue_manager.client.connection, peek_method)
                    potential_job = peek_func()
                    if potential_job and potential_job.id == int(job_id):
                        job = potential_job
                        job_tube = tube
                        break
                except:
                    continue

            if job:
                break

        if not job:
            print(f"Job {job_id} not found in any tube")
            return

        # Get job stats
        stats = queue_manager.client.connection.stats_job(job.id)

        print("\n=== Job Details ===")
        print(f"Job ID: {job.id}")
        print(f"Tube: {job_tube}")
        print(f"State: {stats.get('state', 'unknown')}")
        print(f"Priority: {stats.get('pri', 'N/A')}")
        print(f"Age: {stats.get('age', 0)} seconds")
        print(f"Time to run (TTR): {stats.get('ttr', 0)} seconds")
        print(f"Timeouts: {stats.get('timeouts', 0)}")
        print(f"Releases: {stats.get('releases', 0)}")
        print(f"Buries: {stats.get('buries', 0)}")
        print(f"Kicks: {stats.get('kicks', 0)}")

        print("\n=== Job Data ===")
        try:
            job_data = queue_manager.serializer.deserialize_job(job.body)
            print(json.dumps(job_data, indent=2))
        except Exception as e:
            print(f"Error deserializing job data: {str(e)}")
            print("Raw job body:")
            print(job.body)

    except Exception as e:
        print(f"Error getting job details: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Show beanstalkd queue status')
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show queue statistics')

    # List jobs command
    list_parser = subparsers.add_parser('list', help='List jobs in a specific state')
    list_parser.add_argument('--tube', required=True, help='Tube name')
    list_parser.add_argument('--state', choices=['ready', 'delayed', 'buried'],
                            default='ready', help='Job state to list')
    list_parser.add_argument('--limit', type=int, default=5,
                            help='Maximum number of jobs to list')
    list_parser.add_argument('--ids-only', action='store_true',
                            help='Print only job IDs')

    # Get job details command
    get_parser = subparsers.add_parser('get', help='Get detailed job information')
    get_parser.add_argument('job_id', help='Job ID to inspect')

    args = parser.parse_args()
    logger = setup_logging()

    try:
        queue_manager = QueueManager(host=args.queue_host, port=args.queue_port)

        if args.command == 'stats' or not args.command:
            print_tube_stats(queue_manager)

        elif args.command == 'list':
            list_jobs_by_state(queue_manager, args.tube, args.state,
                             args.limit, args.ids_only)

        elif args.command == 'get':
            get_job_details(queue_manager, args.job_id)

        queue_manager.close()

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())

