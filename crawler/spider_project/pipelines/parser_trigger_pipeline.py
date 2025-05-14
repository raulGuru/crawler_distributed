import logging
from scrapy.exceptions import DropItem
import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from parser.dispatch.job_dispatcher import dispatch_jobs
from lib.utils.logging_utils import LoggingUtils
from lib.storage.mongodb_client import MongoDBClient

# Helper function to convert byte string keys in nested dicts/lists to strings
def convert_keys_to_str(obj):
    if isinstance(obj, dict):
        return { (k.decode('utf-8', 'replace') if isinstance(k, bytes) else k): convert_keys_to_str(v) for k, v in obj.items() }
    elif isinstance(obj, list):
        return [convert_keys_to_str(elem) for elem in obj]
    return obj

class ParserTriggerPipeline:
    """
    Pipeline for initiating parse job dispatch after HTML content is stored.
    It calls parser/dispatch/job_dispatcher.py to handle the actual Beanstalkd interactions.
    """

    def __init__(self, queue_host, queue_port, priority, ttr):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.priority = priority
        self.ttr = ttr
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("ParserTriggerPipeline initialized and logger level set to DEBUG.")

    @classmethod
    def from_crawler(cls, crawler):
        """Get settings from crawler"""
        pipeline = cls(
            queue_host=crawler.settings.get('QUEUE_HOST', 'localhost'),
            queue_port=crawler.settings.get('QUEUE_PORT', 11300),
            priority=crawler.settings.get('PARSE_JOB_PRIORITY', 100),
            ttr=crawler.settings.get('PARSE_JOB_TTR', 180)
        )
        pipeline.logger.info("ParserTriggerPipeline instance created via from_crawler.")
        return pipeline

    def process_item(self, item, spider):
        """Initiate dispatch of parser jobs for stored HTML."""
        self.logger.info(f"ParserTriggerPipeline.process_item received item for URL: {item.get('url', 'N/A')}")

        html_file_path = item.get('html_file_path')
        if not html_file_path:
            self.logger.debug(f"Skipping item, no 'html_file_path': {item.get('url', 'N/A')}")
            return item

        base_parser_data = {
            'url': item.get('url'),
            'html_file_path': html_file_path,
            'domain': item.get('domain'),
            'storage': item.get('storage'),
            'crawled_at': item.get('crawled_at'),
            'crawl_id': item.get('crawl_id')
        }
        base_parser_data = convert_keys_to_str(base_parser_data)

        self.logger.debug(f"Base parser data prepared (crawl_id: {base_parser_data.get('crawl_id')}): {base_parser_data}")

        mongodb_client = None
        inserted_object_id = None
        parser_unique_id = None

        try:
            mongodb_client = MongoDBClient()
            parser_doc_for_insertion = {
                **base_parser_data,
                'processing_status': 'pending_dispatch',
                'parser_jobs_dispatched_at': None,
                'initial_insert_at': datetime.utcnow(),
            }
            self.logger.debug(f"Attempting to insert parser_doc into MongoDB (crawl_id: {parser_doc_for_insertion.get('crawl_id')}): {parser_doc_for_insertion}")
            inserted_object_id = mongodb_client.insert_one('parsed_html_data', parser_doc_for_insertion)

            if inserted_object_id:
                parser_unique_id = str(inserted_object_id)
                self.logger.info(f"Successfully inserted initial parser data for {base_parser_data.get('url')}. MongoDB _id: {parser_unique_id} (ObjectId: {inserted_object_id}).")
            else:
                self.logger.error(f"MongoDB insert_one returned no ID for {base_parser_data.get('url')}. Cannot proceed with dispatch.")
                raise DropItem(f"Failed to get MongoDB ID for {base_parser_data.get('url')}")

        except Exception as e_mongo:
            self.logger.error(f"Error during MongoDB insertion for {base_parser_data.get('url')}: {e_mongo}")
            LoggingUtils.log_exception(self.logger, e_mongo, f"MongoDB insertion failed for {base_parser_data.get('url')}")
            raise DropItem(f"MongoDB error for {base_parser_data.get('url')}: {e_mongo}")
        finally:
            if mongodb_client:
                mongodb_client.close()

        try:
            dispatch_jobs(
                source_parser_item=base_parser_data,
                document_mongo_id=inserted_object_id,
                document_str_id=parser_unique_id
            )
            self.logger.info(f"Successfully initiated parser job dispatch process for {base_parser_data.get('url')} (doc_id: {parser_unique_id}).")
        except Exception as e_dispatch:
            self.logger.error(f"Error calling dispatcher for {base_parser_data.get('url')} (doc_id: {parser_unique_id}): {e_dispatch}")
            LoggingUtils.log_exception(self.logger, e_dispatch, f"Dispatcher failed for {base_parser_data.get('url')}")
            # Depending on policy, we might want to DropItem or try to update MongoDB status to 'dispatch_failed'
            # For now, just log and return the item. The MongoDB doc will remain 'pending_dispatch'.
        return item