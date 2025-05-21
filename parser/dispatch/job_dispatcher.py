import logging
from datetime import datetime
import os
import sys
import time
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

def dispatch_jobs(
    source_parser_item: dict,
    document_mongo_id: any, # Should be ObjectId from pymongo
    document_str_id: str,
):
    """
    Dispatches individual parsing jobs to Beanstalkd for a previously created document in MongoDB.
    The MongoDB document (referenced by document_mongo_id and document_str_id) should have been
    inserted by the calling pipeline (e.g., ParserTriggerPipeline).
    This function then queues jobs for various parser tasks and updates the MongoDB document's
    status upon completion of dispatch.

    Args:
        source_parser_item: Dictionary containing the core data from the crawl (URL, HTML path, etc.).
                            This is used as the base for job payloads.
        document_mongo_id: The MongoDB ObjectId of the document in 'parsed_html_data'.
                           Used for the final status update.
        document_str_id: The string representation of the MongoDB _id.
                         Used as 'document_id' in Beanstalkd job payloads.
    """
    queue_manager = None
    mongodb_client_for_update = None

    source_url = source_parser_item.get('url', 'N/A_URL')
    html_file_path = source_parser_item.get('html_file_path', 'N/A_PATH')

    logger.info(f"dispatch_jobs called for URL: {source_url}, HTML_PATH: {html_file_path}, doc_id: {document_str_id}")
    logger.debug(f"Received source_parser_item: {source_parser_item}, document_mongo_id: {document_mongo_id}")

    if not document_str_id or document_str_id == "None" or not document_mongo_id:
        logger.error(
            f"CRITICAL: Invalid document ID provided for {source_url}. \
            document_str_id: '{document_str_id}', document_mongo_id: '{document_mongo_id}'. Aborting dispatch."
        )
        return

    try:
        logger.info(f"Proceeding to dispatch parser jobs for doc_id '{document_str_id}' ({source_url}).")

        jobs_dispatched_count = 0
        jobs_failed_dispatch_count = 0
        parser_jobs_dispatched_at_utc = datetime.utcnow()

        for task_name_key, task_specific_config in ALL_PARSER_TASK_TYPES.items():

            enqueued_timestamp = datetime.utcnow().isoformat() + "Z"
            job_payload = safe_for_json({
                **source_parser_item,
                "document_id": document_str_id,
                "task_type": task_name_key,
                "enqueued_timestamp": enqueued_timestamp,
                "job_type": "parse",
            })
            logger.debug(f"Prepared job_payload for task_type '{task_name_key}' (doc_id: {document_str_id}): {job_payload}")

            current_priority = task_specific_config.get("priority")
            current_ttr = task_specific_config.get("ttr")
            tube_name = f"crawler_htmlparser_{task_name_key}_tube"

            try:
                queue_manager = QueueManager(host=QUEUE_HOST, port=QUEUE_PORT, logger=logger)

                job_id_from_beanstalkd = queue_manager.enqueue_job(
                    job_data=job_payload,
                    tube=tube_name,
                    priority=current_priority,
                    ttr=current_ttr
                )
                if job_id_from_beanstalkd:
                    logger.info(
                        f"Dispatched job for doc_id {document_str_id} (Beanstalkd ID: {job_id_from_beanstalkd}) "
                        f"task_type '{task_name_key}' to tube '{tube_name}' for {source_url}."
                    )
                    jobs_dispatched_count += 1
                else:
                    logger.warning(
                        f"Failed to dispatch job for doc_id {document_str_id}, task_type '{task_name_key}' to tube '{tube_name}' for {source_url}. "
                        f"No job ID returned from enqueue_job, but no exception raised."
                    )
                    jobs_failed_dispatch_count += 1
            except Exception as e_enqueue:
                logger.error(
                    f"EXCEPTION dispatching job for doc_id {document_str_id}, task_type '{task_name_key}' "
                    f"to tube '{tube_name}' for {source_url}: {str(e_enqueue)}",
                    exc_info=True
                )
                jobs_failed_dispatch_count += 1
            time.sleep(0.02)

        if jobs_failed_dispatch_count > 0:
            logger.warning(f"Completed dispatch attempt for doc_id {document_str_id} ({source_url}): {jobs_dispatched_count} jobs succeeded, {jobs_failed_dispatch_count} failed.")
        else:
            logger.info(f"Successfully dispatched all {jobs_dispatched_count} parser jobs for doc_id {document_str_id} ({source_url}).")

        try:
            mongodb_client_for_update = MongoDBClient(logger=logger)
            update_payload = {
                '$set': {
                    'processing_status': 'dispatch_complete',
                    'parser_job_id': job_id_from_beanstalkd,
                    'jobs_dispatched_total': jobs_dispatched_count,
                    'jobs_failed_dispatch': jobs_failed_dispatch_count,
                    'parser_jobs_dispatched_at': parser_jobs_dispatched_at_utc,
                    'updated_at': datetime.utcnow()
                }
            }
            logger.debug(f"Attempting to update MongoDB for doc_id {document_str_id} (ObjectId: {document_mongo_id}) with payload: {update_payload}")
            mongodb_client_for_update.update_one(
                'parsed_html_data',
                {'_id': document_mongo_id},
                update_payload
            )
            logger.info(f"Successfully updated MongoDB status after dispatch for doc_id {document_str_id} (ObjectId: {document_mongo_id}).")
        except Exception as mongo_update_err:
            logger.error(f"Failed to update MongoDB status after dispatch for doc_id {document_str_id} (ObjectId: {document_mongo_id}): {mongo_update_err}", exc_info=True)
        finally:
            if mongodb_client_for_update:
                mongodb_client_for_update.close()

    except Exception as e:
        logger.error(
            f"CRITICAL error in job dispatch process for doc_id {document_str_id} ({source_url}, file: {html_file_path}): {str(e)}",
            exc_info=True
        )
        raise
    finally:
        if queue_manager:
            queue_manager.close()
            logger.info(f"QueueManager connection closed for doc_id: {document_str_id} dispatch ({source_url}).")
