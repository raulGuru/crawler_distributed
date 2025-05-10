import logging
from datetime import datetime
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config.parser_settings import ALL_PARSER_TASK_TYPES
from lib.storage.mongodb_client import MongoDBClient
from lib.queue.queue_manager import QueueManager
from config.base_settings import QUEUE_HOST, QUEUE_PORT

logger = logging.getLogger(__name__)

def safe_for_json(d):
    out = {}
    for k, v in d.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out

def create_and_dispatch_parser_jobs(
    parser_item: dict,
):
    """
    Creates and dispatches individual parsing jobs to Beanstalkd for a given HTML file.
    It retrieves the MongoDB document ID after an initial insert and uses that for all parser jobs.
    Priority and TTR for jobs are determined from ALL_PARSER_TASK_TYPES in config.parser_settings,

    Args:
        parser_item: Dictionary containing the source data, expected to have keys like
                     'url', 'html_file_path', 'domain', etc.
    """
    queue_manager = None

    source_url = parser_item.get('url', 'N/A_URL')
    html_file_path = parser_item.get('html_file_path', 'N/A_PATH')

    try:
        queue_manager = QueueManager(host=QUEUE_HOST, port=QUEUE_PORT)
        logger.info(f"QueueManager connected to Beanstalkd at {QUEUE_HOST}:{QUEUE_PORT} for dispatching parser jobs for {source_url}.")

        mongodb_client = MongoDBClient()
        parser_doc = {
            **parser_item,
            'processing_status': 'pending_dispatch',
            'parser_jobs_dispatched_at': datetime.utcnow(),
        }

        inserted_object_id = mongodb_client.insert_one('parsed_html_data', parser_doc)
        parser_unique_id = str(inserted_object_id)
        mongodb_client.close()

        logger.info(f"Initial parser data for {source_url} (file: {html_file_path}) stored in MongoDB with _id: {parser_unique_id}.")

        jobs_dispatched_count = 0
        jobs_failed_dispatch_count = 0

        for task_name_key, task_specific_config in ALL_PARSER_TASK_TYPES.items():
            enqueued_timestamp = datetime.utcnow().isoformat() + "Z"

            job_payload = safe_for_json({
                **parser_item,
                "document_id": parser_unique_id,
                "task_type": task_name_key,
                "enqueued_timestamp": enqueued_timestamp,
                "job_type": "parse",
            })

            current_priority = task_specific_config.get("priority")
            current_ttr = task_specific_config.get("ttr")
            tube_name = f"htmlparser_{task_name_key}_tube"

            try:
                job_id_from_beanstalkd = queue_manager.enqueue_job(
                    job_data=job_payload,
                    tube=tube_name,
                    priority=current_priority,
                    ttr=current_ttr
                )
                if job_id_from_beanstalkd:
                    logger.info(
                        f"Dispatched job for doc_id {parser_unique_id} (Beanstalkd ID: {job_id_from_beanstalkd}) "
                        f"task_type '{task_name_key}' to tube '{tube_name}' for {source_url}."
                    )
                    jobs_dispatched_count += 1
                else:
                    logger.warning(
                        f"Failed to dispatch job for doc_id {parser_unique_id}, task_type '{task_name_key}' to tube '{tube_name}' for {source_url}. No job ID returned."
                    )
                    jobs_failed_dispatch_count += 1
            except Exception as e:
                logger.error(
                    f"EXCEPTION dispatching job for doc_id {parser_unique_id}, task_type '{task_name_key}' "
                    f"to tube '{tube_name}' for {source_url}: {str(e)}"
                )
                jobs_failed_dispatch_count += 1

        if jobs_failed_dispatch_count > 0:
            logger.warning(f"Completed dispatch attempt for {source_url}: {jobs_dispatched_count} jobs succeeded, {jobs_failed_dispatch_count} failed.")
        else:
            logger.info(f"Successfully dispatched all {jobs_dispatched_count} parser jobs for {source_url} (doc_id: {parser_unique_id}).")

        try:
            mongodb_client = MongoDBClient()
            mongodb_client.update_one(
                'parsed_html_data',
                {'_id': inserted_object_id},
                {'$set': {'processing_status': 'dispatch_complete', 'jobs_dispatched_total': jobs_dispatched_count, 'jobs_failed_dispatch': jobs_failed_dispatch_count, 'updated_at': datetime.utcnow()}}
            )
            mongodb_client.close()
        except Exception as mongo_update_err:
            logger.error(f"Failed to update MongoDB status after dispatch for doc_id {parser_unique_id}: {mongo_update_err}")

    except Exception as e:
        logger.error(
            f"CRITICAL error in job dispatch process for {source_url} (file: {html_file_path}): {str(e)}"
        )
        raise
    finally:
        if queue_manager:
            queue_manager.close()
            logger.info(f"QueueManager connection closed for {source_url} dispatch.")
