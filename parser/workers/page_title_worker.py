import os
import sys
import time
from datetime import datetime
import argparse

# Patch sys.path for direct script execution
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from bs4 import BeautifulSoup
from bson import ObjectId
from bson.errors import InvalidId
from parser.workers.base_parser_worker import BaseParserWorker, RetryableError, NonRetryableError
from config.base_settings import QUEUE_HOST, QUEUE_PORT, DB_URI

class PageTitleWorker(BaseParserWorker):
    def __init__(self, instance_id: int = 0, queue_host: str = None, queue_port: int = None, db_uri: str = None):
        # BaseParserWorker will use its own defaults from settings if these are None
        # Or it could be modified to accept these and pass them to QueueManager/MongoDBClient if needed.
        # For now, we just accept them in __init__ so argparse doesn't break if WorkerManager sends them.
        # The actual values for queue/db are picked up by BaseParserWorker from settings.
        super().__init__(
            tube_name="htmlparser_page_title_extraction_tube",
            task_type="page_title_extraction",
            instance_id=instance_id
            # If BaseParserWorker was changed to take queue/db args:
            # queue_host=queue_host or QUEUE_HOST,
            # queue_port=queue_port or QUEUE_PORT,
            # db_uri=db_uri or DB_URI
        )

    def process_task(self, job_data: dict) -> None:
        doc_id_str = job_data["document_id"]
        html_path = job_data["html_file_path"]

        self.logger.debug(f"Processing task for doc_id: {doc_id_str}, html_path: {html_path}")

        if not os.path.exists(html_path):
            self.logger.error(f"HTML file not found: {html_path} for doc_id: {doc_id_str}")
            raise NonRetryableError(f"HTML file not found: {html_path}")

        try:
            with open(html_path, "r", encoding="utf-8") as f:
                html_content = f.read()
        except Exception as e:
            self.logger.error(f"Failed to read HTML file {html_path} for doc_id: {doc_id_str}: {e}")
            raise RetryableError(f"Failed to read HTML file {html_path}: {e}")

        try:
            soup = BeautifulSoup(html_content, "lxml")
            title_tag = soup.title
            title_text = title_tag.string.strip() if title_tag and title_tag.string else None
            title_length = len(title_text) if title_text else 0

            # This aligns with a simplified part of PARSER_MONGODB_SCHEMA_crawler_snakeCase.md -> page_title
            # A full implementation would extract more fields (meta_keywords, canonical_url, etc.)
            page_title_data_to_store = {
                "title": title_text,
                "title_length": title_length,
                # "meta_keywords": None, # Example for future extension
                # "canonical_url": None,
                # "robots": None,
                # "hreflang_tags": [],
                # "og_tags": {},
                # "twitter_tags": {}
            }
            self.logger.debug(f"Extracted page_title_data: {page_title_data_to_store} for doc_id: {doc_id_str}")

        except Exception as e:
            self.logger.error(f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}")
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

        try:
            mongo_document_id = ObjectId(doc_id_str)
        except InvalidId:
            self.logger.error(f"Invalid document_id format: {doc_id_str}. Cannot convert to ObjectId.")
            raise NonRetryableError(f"Invalid document_id format: {doc_id_str}")

        # Prepare the MongoDB update payload
        current_utc_time = datetime.utcnow()
        update_payload = {
            "$set": {
                "page_title": page_title_data_to_store,
                f"worker_completion_timestamps.{self.task_type}": current_utc_time,
                "last_updated_at": current_utc_time
            },
            "$setOnInsert": { # Fields to set only if a new document is created by upsert
                "first_processed_at": current_utc_time,
                # "source_url": job_data.get("parser_item", {}).get("url", None) # If parser_item is in job_data
            }
            # Consider adding to processing_status if needed, e.g., add "page_title_extraction_complete" to a set
            # Or update processing_status to "partial"
            # "$addToSet": {"processing_events": f"{self.task_type}_completed"}
        }

        if "parser_item" in job_data and "source_url" in job_data["parser_item"]:
            update_payload["$setOnInsert"]["source_url"] = job_data["parser_item"]["source_url"]
        if "parser_item" in job_data and "original_file_path" in job_data["parser_item"]:
             update_payload["$setOnInsert"]["original_file_path"] = job_data["parser_item"]["original_file_path"]


        self.logger.debug(f"Preparing to update MongoDB for doc_id: {mongo_document_id} with payload: {update_payload}")

        try:
            # Assuming self.mongodb_client.update_one is compatible with pymongo's update_one
            # and the MongoDBClient class provides a method to get the collection.
            # For example: collection = self.mongodb_client.get_collection("parsed_html_data")
            # result = collection.update_one(...)
            # Using a generic call assuming the client has a direct update_one method:
            db_result = self.mongodb_client.update_one(
                collection_name="parsed_html_data",
                query={"_id": mongo_document_id},
                update=update_payload,
                upsert=True
            )

            # Check the result of the update operation
            # Pymongo's UpdateResult has matched_count, modified_count, upserted_id
            # Assuming db_result is an UpdateResult object or a dict-like wrapper
            matched_count = db_result.matched_count if hasattr(db_result, 'matched_count') else db_result.get('matched_count', 0)
            modified_count = db_result.modified_count if hasattr(db_result, 'modified_count') else db_result.get('modified_count', 0)
            upserted_id = db_result.upserted_id if hasattr(db_result, 'upserted_id') else db_result.get('upserted_id')

            if matched_count == 0 and upserted_id is None:
                self.logger.error(f"MongoDB upsert failed for doc_id {doc_id_str}. No match and no upsert.")
                raise RetryableError(f"MongoDB upsert failed for doc_id {doc_id_str}: No match and no upsert_id.")
            elif upserted_id:
                self.logger.info(f"Successfully upserted (created) document for doc_id_str {doc_id_str} (MongoDB _id: {upserted_id}) with page_title data.")
            elif matched_count > 0 and modified_count > 0:
                self.logger.info(f"Successfully updated document for doc_id_str {doc_id_str} (MongoDB _id: {mongo_document_id}) with page_title data.")
            elif matched_count > 0 and modified_count == 0:
                self.logger.info(f"Document for doc_id_str {doc_id_str} (MongoDB _id: {mongo_document_id}) was matched but not modified (data might be the same). Consider this a success.")
            else:
                 self.logger.warning(f"MongoDB update for doc_id {doc_id_str} resulted in an unexpected state: matched={matched_count}, modified={modified_count}, upserted_id={upserted_id}")


        except Exception as e:
            self.logger.error(f"MongoDB update failed for doc_id {doc_id_str}: {e}")
            raise RetryableError(f"MongoDB update failed for doc_id {doc_id_str}: {e}")

        self.logger.info(f"Successfully processed and updated page_title for doc_id: {doc_id_str}")

if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description='Page Title Parser Worker')
    cli_parser.add_argument(
        '--instance-id',
        type=int,
        default=0,
        help='Instance ID for this worker'
    )
    # Add other arguments that WorkerManager might pass, so argparse doesn't complain
    cli_parser.add_argument('--queue-host', type=str, default=QUEUE_HOST, help='Queue host (primarily for WorkerManager compatibility)')
    cli_parser.add_argument('--queue-port', type=int, default=QUEUE_PORT, help='Queue port (primarily for WorkerManager compatibility)')
    cli_parser.add_argument('--db-uri', type=str, default=DB_URI, help='MongoDB URI (primarily for WorkerManager compatibility)')

    args = cli_parser.parse_args()

    # Pass the parsed args to the constructor
    # PageTitleWorker itself might not use queue_host, queue_port, db_uri directly
    # if BaseParserWorker uses global settings, but they are accepted to avoid errors.
    worker = PageTitleWorker(
        instance_id=args.instance_id,
        queue_host=args.queue_host,
        queue_port=args.queue_port,
        db_uri=args.db_uri
    )
    worker.start()