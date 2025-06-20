#!/usr/bin/env python3
import os
import sys

# Add project root to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.append(PROJECT_ROOT)

import argparse
import logging
import json
from dateutil import parser as date_parser
from tabulate import tabulate


from lib.queue.queue_manager import QueueManager
from config.base_settings import LOG_DIR, QUEUE_HOST, QUEUE_PORT


def setup_logging():
    """Set up logging"""
    logger = logging.getLogger('JobStatus')
    logger.setLevel(logging.INFO)

    # Create log directory if it doesn't exist
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(console_handler)

    return logger


def format_date(date_str):
    """Format date string for display"""
    if not date_str:
        return "N/A"
    try:
        if isinstance(date_str, str):
            dt = date_parser.parse(date_str)
        else:
            dt = date_str
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(date_str)


def format_duration(seconds):
    """Format duration in seconds to a human-readable string"""
    if seconds is None:
        return "N/A"

    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    parts = []
    if days > 0:
        parts.append(f"{int(days)}d")
    if hours > 0 or days > 0:
        parts.append(f"{int(hours)}h")
    if minutes > 0 or hours > 0 or days > 0:
        parts.append(f"{int(minutes)}m")
    parts.append(f"{int(seconds)}s")

    return " ".join(parts)


def format_job_data(job_data):
    """Format job data for display"""
    if not job_data:
        return "N/A"

    try:
        formatted = {}
        # Extract key fields
        if isinstance(job_data, dict):
            formatted = {
                "domain": job_data.get("domain", "N/A"),
                "url": job_data.get("url", "N/A"),
                "max_pages": job_data.get("max_pages", "N/A"),
                "single_url": job_data.get("single_url", "N/A"),
                "use_sitemap": job_data.get("use_sitemap", "N/A")
            }
        return json.dumps(formatted, indent=2)
    except Exception:
        return str(job_data)


def peek_jobs_in_tube(client, tube, state, limit):
    """Peek at jobs in a tube by state (ready, delayed, buried) up to limit"""
    jobs = []
    peek_method = {
        'ready': client.peek_ready,
        'delayed': client.peek_delayed,
        'buried': client.peek_buried
    }[state]
    for _ in range(limit):
        job = peek_method(tube)
        if not job:
            break
        jobs.append(job)
        break  # Only one job can be peeked at a time per state in beanstalkd
    return jobs


def list_recent_jobs(queue_manager, limit):
    """List recent jobs from Beanstalkd (limited info) in table format"""
    print("\n=== Recent Jobs (Beanstalkd) ===")
    tubes = queue_manager.client.tubes()
    table = []
    headers = ["Tube", "Job ID", "Domain", "URL", "Max Pages", "Single URL", "Use Sitemap"]
    for tube in tubes:
        jobs = peek_jobs_in_tube(queue_manager.client, tube, 'ready', limit)
        for job in jobs:
            try:
                job_data = queue_manager.serializer.deserialize_job(job.body)
                table.append([
                    tube,
                    getattr(job, 'id', 'N/A'),
                    job_data.get("domain", "N/A"),
                    job_data.get("url", "N/A"),
                    job_data.get("max_pages", "N/A"),
                    job_data.get("single_url", "N/A"),
                    job_data.get("use_sitemap", "N/A")
                ])
            except Exception as e:
                table.append([tube, getattr(job, 'id', 'N/A'), f"Failed to deserialize: {e}", '', '', '', ''])
        if not jobs:
            table.append([tube, "-", "No ready jobs.", '', '', '', ''])
    print(tabulate(table, headers=headers, tablefmt="github"))


def list_jobs_by_status(queue_manager, status, limit):
    print(f"\n=== Jobs with status '{status}' (Beanstalkd) ===")
    tubes = queue_manager.client.tubes()
    headers = ["Tube", "Job ID", "Domain", "URL", "Max Pages", "Single URL", "Use Sitemap"]
    table = []
    state_map = {
        'pending': 'ready',
        'running': 'reserved',
        'completed': 'delayed',  # Not a perfect mapping
        'failed': 'buried'
    }
    state = state_map.get(status, 'ready')
    for tube in tubes:
        if state == 'reserved':
            # Beanstalkd does not support peeking reserved jobs directly
            table.append([tube, '-', 'Cannot peek reserved jobs (running) in Beanstalkd', '', '', '', ''])
            continue
        if state not in ['ready', 'delayed', 'buried']:
            table.append([tube, '-', f"No jobs with status '{status}'.", '', '', '', ''])
            continue
        jobs = peek_jobs_in_tube(queue_manager.client, tube, state, limit)
        for job in jobs:
            try:
                job_data = queue_manager.serializer.deserialize_job(job.body)
                table.append([
                    tube,
                    getattr(job, 'id', 'N/A'),
                    job_data.get("domain", "N/A"),
                    job_data.get("url", "N/A"),
                    job_data.get("max_pages", "N/A"),
                    job_data.get("single_url", "N/A"),
                    job_data.get("use_sitemap", "N/A")
                ])
            except Exception as e:
                table.append([tube, getattr(job, 'id', 'N/A'), f"Failed to deserialize: {e}", '', '', '', ''])
        if not jobs and state != 'reserved':
            table.append([tube, "-", f"No jobs with status '{status}'.", '', '', '', ''])
    print(tabulate(table, headers=headers, tablefmt="github"))


def get_job_details(queue_manager, crawl_id):
    print(f"\n=== Job Details for crawl_id {crawl_id} (Beanstalkd) ===")
    tubes = queue_manager.client.tubes()
    found = False
    for tube in tubes:
        for state in ['ready', 'delayed', 'buried']:
            jobs = peek_jobs_in_tube(queue_manager.client, tube, state, 1)
            for job in jobs:
                try:
                    job_data = queue_manager.serializer.deserialize_job(job.body)
                    if str(job_data.get('crawl_id')) == str(crawl_id):
                        print(f"Found in tube '{tube}' (state: {state}):\n  Job ID: {job.id}, Data: {format_job_data(job_data)}")
                        found = True
                except Exception as e:
                    print(f"  Job ID: {job.id}, Failed to deserialize: {e}")
    if not found:
        print("No job found with the given crawl_id.")


def list_all_jobs_with_status(queue_manager, limit=10):
    print("\n=== All Jobs with Status (Beanstalkd) ===")
    tubes = queue_manager.client.tubes()
    headers = [
        "Tube", "Job ID", "Status", "Domain", "URL", "Max Pages", "Single URL", "Use Sitemap"
    ]
    table = []
    state_peek_map = {
        'ready': queue_manager.client.peek_ready,
        'delayed': queue_manager.client.peek_delayed,
        'buried': queue_manager.client.peek_buried
    }
    for tube in tubes:
        for state, peek_func in state_peek_map.items():
            for _ in range(limit):
                job = peek_func(tube)
                if not job:
                    if _ == 0:
                        table.append([
                            tube, "-", state, f"No {state} jobs", '', '', '', ''
                        ])
                    break
                try:
                    job_data = queue_manager.serializer.deserialize_job(job.body)
                    table.append([
                        tube,
                        getattr(job, 'id', 'N/A'),
                        state,
                        job_data.get("domain", "N/A"),
                        job_data.get("url", "N/A"),
                        job_data.get("max_pages", "N/A"),
                        job_data.get("single_url", "N/A"),
                        job_data.get("use_sitemap", "N/A")
                    ])
                except Exception as e:
                    table.append([
                        tube, getattr(job, 'id', 'N/A'), state,
                        f"Failed to deserialize: {e}", '', '', '', ''
                    ])
                break  # Only one job can be peeked at a time per state in beanstalkd
        # Add a row for running jobs (reserved) - not available in Beanstalkd
        table.append([
            tube, "-", "reserved",
            "[RUNNING] Cannot list reserved jobs (running) due to Beanstalkd limitation. See summary below for running job counts.", '', '', '', ''
        ])
    print(tabulate(table, headers=headers, tablefmt="github"))

    # Add summary of job counts per tube (including reserved/running jobs)
    print("\n--- Job State Summary (per tube) ---")
    stats = queue_manager.get_stats()
    summary_headers = ["Tube", "Ready", "Reserved (Running)", "Delayed", "Buried", "Total Jobs"]
    summary_table = []
    for tube, tube_stats in stats['tubes'].items():
        summary_table.append([
            tube,
            tube_stats['ready'],
            tube_stats['reserved'],
            tube_stats['delayed'],
            tube_stats['buried'],
            tube_stats['total']
        ])
    # Add a total row
    summary_table.append([
        "TOTAL",
        stats['ready_jobs'],
        stats['reserved_jobs'],
        stats['delayed_jobs'],
        stats['buried_jobs'],
        stats['total_jobs']
    ])
    print(tabulate(summary_table, headers=summary_headers, tablefmt="github"))
    print("\nNote: Reserved jobs are currently running and cannot be listed due to Beanstalkd limitations. Only their counts are shown above.")


def display_detailed_beanstalkd_view(queue_manager):
    """Display the comprehensive monitoring view from QueueManager."""
    print("\n=== Detailed Beanstalkd Monitoring View ===")
    try:
        view_data = queue_manager.get_detailed_monitoring_view()

        if not view_data:
            print("Failed to retrieve detailed monitoring view.")
            return

        # Server Stats
        print("\n--- Server Statistics ---")
        if view_data.get('server_stats') and not view_data['server_stats'].get('error'):
            for key, value in view_data['server_stats'].items():
                print(f"  {key}: {value}")
        elif view_data.get('server_stats', {}).get('error'):
            print(f"  Error fetching server stats: {view_data['server_stats']['error']}")
        else:
            print("  No server stats available.")

        if view_data.get('error_listing_tubes'):
            print(f"\nError listing tubes: {view_data['error_listing_tubes']}")
            return

        # Tubes Details
        print("\n--- Tubes Details ---")
        if not view_data.get('tubes_details'):
            print("  No tubes found or no details available.")
            return

        for tube_name, details in view_data['tubes_details'].items():
            print(f"\n--- Tube: {tube_name} ---")

            # Tube Stats
            print("  Tube Statistics:")
            if details.get('stats') and not details['stats'].get('error'):
                # Use tabulate for tube stats for better alignment
                tube_stats_table = []
                headers = ["Statistic", "Value"]
                for key, value in details['stats'].items():
                    tube_stats_table.append([key, value])
                print(tabulate(tube_stats_table, headers=headers, tablefmt="plain", stralign="left"))
            elif details.get('stats', {}).get('error'):
                print(f"    Error fetching stats for tube {tube_name}: {details['stats']['error']}")
            else:
                print("    No stats available for this tube.")

            # Peeked Jobs
            for job_type_key in ['peeked_ready_job', 'peeked_delayed_job', 'peeked_buried_job']:
                job_info = details.get(job_type_key)
                job_type_name = job_type_key.replace('peeked_', '').replace('_job', '').capitalize()
                print(f"  Peeked {job_type_name} Job:")
                if job_info and not job_info.get('error'):
                    print(f"    Job ID: {job_info.get('id', 'N/A')}")
                    print("    Body:")
                    if job_info.get('body') and not job_info['body'].get('error_deserializing'):
                        print(json.dumps(job_info['body'], indent=2).replace("\n", "\n      "))
                    elif job_info.get('body', {}).get('error_deserializing'):
                        print(f"      Error deserializing: {job_info['body']['error_deserializing']}")
                        if 'raw_body' in job_info['body']:
                             print(f"      Raw Body: {job_info['body']['raw_body']}")
                    else:
                        print("      N/A or empty")

                    print("    Job Stats:")
                    if job_info.get('job_stats') and not job_info['job_stats'].get('error'):
                        # Use tabulate for job stats for better alignment
                        job_stats_table = []
                        js_headers = ["Stat", "Value"]
                        for key, value in job_info['job_stats'].items():
                            job_stats_table.append([key, value])
                        print(tabulate(job_stats_table, headers=js_headers, tablefmt="plain", stralign="left"))
                    elif job_info.get('job_stats', {}).get('error'):
                        print(f"      Error fetching job stats: {job_info['job_stats']['error']}")
                    else:
                        print("      No job stats available.")
                elif job_info and job_info.get('error'):
                    print(f"    Error peeking {job_type_name.lower()} job: {job_info['error']}")
                else:
                    print("    No job found in this state.")
    except Exception as e:
        print(f"An error occurred while generating the detailed view: {str(e)}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Check job status (Beanstalkd only)')
    parser.add_argument('--queue-host', default=QUEUE_HOST, help='Beanstalkd host')
    parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Beanstalkd port')
    parser.add_argument('--all-jobs', action='store_true', help='List all jobs with status')
    parser.add_argument('--full-monitor', action='store_true', help='Display a full, detailed monitoring view of Beanstalkd')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    list_parser = subparsers.add_parser('list', help='List recent jobs')
    list_parser.add_argument('--limit', type=int, default=10, help='Maximum number of jobs to show')
    status_parser = subparsers.add_parser('status', help='List jobs by status')
    status_parser.add_argument('--status', choices=['pending', 'running', 'completed', 'failed'], required=True, help='Job status to filter by')
    status_parser.add_argument('--limit', type=int, default=10, help='Maximum number of jobs to show')
    get_parser = subparsers.add_parser('get', help='Get details of a specific job')
    get_parser.add_argument('crawl_id', help='Crawl job ID')
    args = parser.parse_args()
    logger = setup_logging()
    try:
        queue_manager = QueueManager(host=args.queue_host, port=args.queue_port)
        if args.command == 'list':
            list_recent_jobs(queue_manager, args.limit)
        elif args.command == 'status':
            list_jobs_by_status(queue_manager, args.status, args.limit)
        elif args.command == 'get':
            get_job_details(queue_manager, args.crawl_id)
        elif args.all_jobs:
            list_all_jobs_with_status(queue_manager, limit=args.limit if hasattr(args, 'limit') else 10)
            queue_manager.close()
            return 0
        elif args.full_monitor:
            display_detailed_beanstalkd_view(queue_manager)
            queue_manager.close()
            return 0
        else:
            parser.print_help()
        queue_manager.close()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())