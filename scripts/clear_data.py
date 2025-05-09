import sys
from pymongo import MongoClient
import os
import greenstalk
import psutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.base_settings import DB_URI, QUEUE_HOST, QUEUE_PORT, QUEUE_TUBES, DATA_DIR, HTML_DIR, LOG_DIR

def clear_mongodb():
    """Clear all collections in the MongoDB database"""
    print("Clearing MongoDB collections...")
    client = MongoClient(DB_URI)
    db = client.get_database()

    # Print collections before clearing
    collections = db.list_collection_names()
    print(f"Found collections: {collections}")

    # Clear each collection
    for collection in collections:
        count = db[collection].count_documents({})
        db[collection].delete_many({})
        print(f"Cleared {count} documents from {collection}")

    print("MongoDB collections cleared.")

def clear_beanstalkd():
    """Clear all jobs from Beanstalkd queue"""
    print("Clearing Beanstalkd queues...")

    try:
        # Connect to Beanstalkd - greenstalk uses address instead of host/port
        client = greenstalk.Client((QUEUE_HOST, QUEUE_PORT))

        # List of tubes to clear
        tubes = QUEUE_TUBES

        for tube in tubes:
            try:
                # Switch to the tube
                client.use(tube)
                client.watch(tube)

                # Clear jobs
                jobs_cleared = 0
                while True:
                    try:
                        # Try to reserve a job with a timeout of 0 seconds
                        job = client.reserve(timeout=0)
                        client.delete(job)
                        jobs_cleared += 1
                    except greenstalk.TimedOutError:
                        # No more jobs to reserve
                        break

                print(f"Cleared {jobs_cleared} jobs from tube {tube}")

            except Exception as e:
                print(f"Error clearing tube {tube}: {str(e)}")

        client.close()
        print("Beanstalkd queues cleared.")

    except Exception as e:
        print(f"Error connecting to Beanstalkd: {str(e)}")

def clear_html_files():
    """Clear all HTML files from the data directory"""
    print("Clearing HTML files...")

    html_dir = os.path.join(DATA_DIR, HTML_DIR)
    if os.path.exists(html_dir):
        # Get subdirectories (domains)
        domains = [d for d in os.listdir(html_dir)
                  if os.path.isdir(os.path.join(html_dir, d))]

        for domain in domains:
            domain_dir = os.path.join(html_dir, domain)
            files = os.listdir(domain_dir)
            for file in files:
                os.remove(os.path.join(domain_dir, file))
            print(f"Cleared {len(files)} files from {domain}")
            os.rmdir(domain_dir)
            print(f"Cleared {domain} directory")

        print("HTML files cleared.")
    else:
        print(f"HTML directory {html_dir} not found.")

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
                    'integration_service', 'monitor_worker', 'parse_worker', 'crawl_job_listener', 'spider_project', 'submit_crawl_job', 'run_integration.sh']):
                    print(f"Killing process PID {proc.info['pid']}: {cmdline}")
                    proc.kill()
                    killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    print(f"Killed {killed} related Python processes.")

def clear_log_files():
    """Clear all log files including integration_service.log"""
    print("Clearing log files...")
    log_dir = os.path.join(DATA_DIR, LOG_DIR)
    if os.path.exists(log_dir):
        current_files = os.listdir(log_dir)
        for file in current_files:
            file_path = os.path.join(log_dir, file)
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Could not remove {file}: {str(e)}")
        print(f"Cleared {len(current_files)} log files.")
    else:
        print(f"Log directory {log_dir} not found.")

if __name__ == "__main__":
    print("Clearing all data to start fresh...")
    # kill_related_python_processes()
    clear_mongodb()
    clear_beanstalkd()
    clear_html_files()
    clear_log_files()
    print("Done! System is now ready for fresh crawling.")