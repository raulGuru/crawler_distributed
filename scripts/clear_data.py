import sys
from pymongo import MongoClient
import os
import greenstalk
import psutil
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.base_settings import (
    QUEUE_HOST, QUEUE_PORT, QUEUE_CRAWL_TUBE, HTML_DIR, LOG_DIR, MONGO_URI,
    INTEGRATION_SERVICE_LOG_DIR, SUBMIT_CRAWL_JOBS_DIR, CRAWL_JOB_LISTENERS_DIR,
    SCRAPY_LOGS_DIR, PARSER_WORKERS_DIR, HEALTH_CHECKS_DIR
)
from config.parser_settings import ALL_PARSER_TASK_TYPES

def clear_mongodb():
    """Clear all collections in the MongoDB database"""
    print("Clearing MongoDB collections...")
    client = MongoClient(MONGO_URI)
    db = client.get_database()
    collections = db.list_collection_names()
    print(f"Found collections: {collections}")
    for collection in collections:
        count = db[collection].count_documents({})
        db[collection].delete_many({})
        print(f"Cleared {count} documents from {collection}")
    print("MongoDB collections cleared.")

def clear_beanstalkd():
    """Clear all jobs from Beanstalkd queue"""
    print("Clearing Beanstalkd queues...")
    try:
        client = greenstalk.Client((QUEUE_HOST, QUEUE_PORT))
        parser_task_tubes = [f"crawler_htmlparser_{task_type}_tube" for task_type in ALL_PARSER_TASK_TYPES.keys()]
        # QUEUE_CRAWL_TUBE may be a string or list; normalize to list
        if isinstance(QUEUE_CRAWL_TUBE, str):
            all_tubes_to_clear = [QUEUE_CRAWL_TUBE] + parser_task_tubes
        else:
            all_tubes_to_clear = list(set(list(QUEUE_CRAWL_TUBE) + parser_task_tubes))
        print(f"Targeting Beanstalkd tubes for clearing: {all_tubes_to_clear}")
        for tube in all_tubes_to_clear:
            try:
                client.use(tube)
                client.watch(tube)
                jobs_cleared = 0
                while True:
                    try:
                        job = client.reserve(timeout=0)
                        if job:
                            client.delete(job)
                            jobs_cleared += 1
                        else:
                            break
                    except greenstalk.TimedOutError:
                        break
                    except greenstalk.NotFoundError:
                        print(f"Tube {tube} not found or empty during reserve, skipping.")
                        break
                    except Exception as e_reserve:
                        print(f"Error processing job in tube {tube}: {str(e_reserve)}")
                        break
                print(f"Cleared {jobs_cleared} jobs from tube {tube}")
                try:
                    client.ignore(tube)
                except greenstalk.NotFoundError:
                    pass
            except greenstalk.NotFoundError:
                print(f"Tube {tube} not found, skipping.")
            except Exception as e:
                print(f"Error clearing tube {tube}: {str(e)}")
        client.close()
        print("Beanstalkd queues cleared.")
    except Exception as e:
        print(f"Error connecting to Beanstalkd: {str(e)}")

def clear_html_files():
    """Clear all HTML files and subdirectories from the HTML_DIR directory (per new config)"""
    print("Clearing HTML files...")
    html_dir = HTML_DIR
    if os.path.exists(html_dir):
        domains = [d for d in os.listdir(html_dir)
                  if os.path.isdir(os.path.join(html_dir, d))]
        total_files = 0
        total_dirs = 0
        for domain in domains:
            domain_dir = os.path.join(html_dir, domain)
            file_count, dir_count = _count_files_and_dirs(domain_dir)
            total_files += file_count
            total_dirs += dir_count + 1  # include the domain dir itself
            shutil.rmtree(domain_dir)
            print(f"  Cleared {file_count} files, {dir_count+1} dirs in {domain}")
        print(f"HTML: Cleared {total_files} files and {total_dirs} directories in total.")
    else:
        print(f"HTML directory {html_dir} not found.")

def _count_files_and_dirs(directory):
    file_count = 0
    dir_count = 0
    for root, dirs, files in os.walk(directory):
        file_count += len(files)
        if root != directory:
            dir_count += 1
    return file_count, dir_count

def clear_log_files():
    """Clear all log files and subdirectories in LOG_DIR (per new config)"""
    print("Clearing log files...")
    log_dirs = [LOG_DIR, INTEGRATION_SERVICE_LOG_DIR, SUBMIT_CRAWL_JOBS_DIR, CRAWL_JOB_LISTENERS_DIR, SCRAPY_LOGS_DIR, PARSER_WORKERS_DIR, HEALTH_CHECKS_DIR]
    total_files = 0
    total_dirs = 0
    for log_dir in log_dirs:
        if os.path.exists(log_dir):
            # Remove all subdirs and files inside log_dir, but not log_dir itself
            for entry in os.listdir(log_dir):
                entry_path = os.path.join(log_dir, entry)
                if os.path.isfile(entry_path):
                    try:
                        os.remove(entry_path)
                        total_files += 1
                    except Exception as e:
                        print(f"Could not remove file {entry_path}: {str(e)}")
                elif os.path.isdir(entry_path):
                    try:
                        sub_file_count, sub_dir_count = _count_files_and_dirs(entry_path)
                        shutil.rmtree(entry_path)
                        total_files += sub_file_count
                        total_dirs += sub_dir_count + 1  # include the subdir itself
                        print(f"  Cleared {sub_file_count} files, {sub_dir_count+1} dirs in {entry_path}")
                    except Exception as e:
                        print(f"Could not remove dir {entry_path}: {str(e)}")
            print(f"Log: Cleared files and subdirs in {log_dir}.")
        else:
            print(f"Log directory {log_dir} not found.")
    print(f"Logs: Cleared {total_files} files and {total_dirs} directories in total.")

def kill_related_python_processes():
    """Kill all Python processes related to the crawler project"""
    print("\nWARNING: This will kill all Python processes related to the crawler (integration service, workers, etc.)!")
    # Removed confirmation prompt for automation
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] == 'python' or proc.info['name'] == 'python3':
                cmdline = ' '.join(proc.info['cmdline'])
                if any(keyword in cmdline for keyword in [
                    'integration_service', 'monitor_worker', 'crawl_job_listener', 'spider_project', 'submit_crawl_job', 'run_integration.sh']):
                    print(f"Killing process PID {proc.info['pid']}: {cmdline}")
                    proc.kill()
                    killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    print(f"Killed {killed} related Python processes.")

if __name__ == "__main__":
    print("Clearing all data to start fresh...")
    # kill_related_python_processes()
    clear_mongodb()
    clear_beanstalkd()
    clear_html_files()
    clear_log_files()
    print("Done! System is now ready for fresh crawling.")