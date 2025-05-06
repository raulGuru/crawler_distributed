import logging
import json
from scrapy.exceptions import DropItem
import sys
import os

# Add the project root to the path to import from lib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from lib.queue.beanstalkd_client import BeanstalkdClient
from lib.queue.job_serializer import JobSerializer


class QueuePipeline:
    """
    Pipeline for enqueueing parse jobs after HTML content is stored
    """

    def __init__(self, beanstalkd_host, beanstalkd_port, parse_tube, priority, ttr):
        self.beanstalkd_host = beanstalkd_host
        self.beanstalkd_port = beanstalkd_port
        self.parse_tube = parse_tube
        self.priority = priority
        self.ttr = ttr
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None
        self.serializer = JobSerializer()

    @classmethod
    def from_crawler(cls, crawler):
        """Get settings from crawler"""
        return cls(
            beanstalkd_host=crawler.settings.get('BEANSTALKD_HOST', 'localhost'),
            beanstalkd_port=crawler.settings.get('BEANSTALKD_PORT', 11300),
            parse_tube=crawler.settings.get('PARSE_TUBE', 'parse_jobs'),
            priority=crawler.settings.get('PARSE_JOB_PRIORITY', 100),
            ttr=crawler.settings.get('PARSE_JOB_TTR', 180)  # 3 minutes
        )

    def open_spider(self, spider):
        """Initialize beanstalkd client when spider opens"""
        try:
            self.client = BeanstalkdClient(
                host=self.beanstalkd_host,
                port=self.beanstalkd_port
            )
            self.client.use_tube(self.parse_tube)
            self.logger.info(f"QueuePipeline connected to beanstalkd at {self.beanstalkd_host}:{self.beanstalkd_port} using tube {self.parse_tube}")
        except Exception as e:
            self.logger.error(f"Failed to connect to beanstalkd: {str(e)}")
            raise

    def close_spider(self, spider):
        """Close beanstalkd client when spider closes"""
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        """Enqueue parse job for stored HTML"""
        # Skip if HTML was not stored
        if not item.get('html_file_path'):
            return item

        # Create parse job data
        job_data = {
            'job_type': 'parse',
            'url': item['url'],
            'html_file_path': item['html_file_path'],
            'timestamp': item.get('timestamp'),
            'spider_name': spider.name,
            'domain': getattr(spider, 'domain', None),
            'crawl_id': getattr(spider, 'crawl_id', None)
        }

        try:
            # Serialize and enqueue job
            serialized_job = self.serializer.serialize_job(job_data)
            if self.client:
                job_id = self.client.put(
                    serialized_job,
                    priority=self.priority,
                    ttr=self.ttr
                )
                self.logger.debug(f"Enqueued parse job {job_id} for {item['url']}")

                # Add job_id to item
                item['parse_job_id'] = job_id
            else:
                self.logger.error("Beanstalkd client not initialized")

        except Exception as e:
            self.logger.error(f"Error enqueueing parse job: {str(e)}")

        return item