import logging
from scrapy.exceptions import DropItem
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from parser.dispatch.job_dispatcher import create_and_dispatch_parser_jobs
from lib.utils.logging_utils import LoggingUtils

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

        # Selectively build parser_item with necessary fields
        raw_parser_item = {
            'url': item.get('url'),
            'html_file_path': html_file_path,
            'domain': item.get('domain'),
            'storage': item.get('storage'),
            'crawled_at': item.get('crawled_at'),
            'crawl_id': item.get('crawl_id')
        }

        # Ensure all keys and sub-keys are strings
        parser_item = convert_keys_to_str(raw_parser_item)

        self.logger.debug(f"Prepared parser_item for dispatch (crawl_id: {parser_item.get('crawl_id')}): {parser_item}")

        try:
            create_and_dispatch_parser_jobs(parser_item)
            self.logger.info(f"Successfully initiated parser job dispatch for {parser_item.get('url')} (file: {parser_item.get('html_file_path')}).")
        except Exception as e:
            self.logger.error(f"Error calling create_and_dispatch_parser_jobs for {parser_item.get('url')} (file: {parser_item.get('html_file_path')}): {e}")
            LoggingUtils.log_exception(self.logger, e, f"create_and_dispatch_parser_jobs failed for {parser_item.get('url')}")
            # Optionally, re-raise or DropItem if this failure is critical
            # raise DropItem(f"Failed to dispatch parser jobs for {item.get('url')}")
        return item