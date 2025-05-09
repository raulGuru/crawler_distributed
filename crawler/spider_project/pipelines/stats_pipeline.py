import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pymongo import MongoClient
from scrapy import Spider
from urllib.parse import urlparse
from config.base_settings import MONGO_CRAWL_JOB_COLLECTION

logger = logging.getLogger(__name__)

class StatsPipeline:
    def __init__(self, mongodb_uri: str):
        """Initialize the pipeline with MongoDB connection details"""
        self.mongodb_uri = mongodb_uri
        self.mongo_client = None
        self.crawl_jobs = None
        self.stats = {
            'pages_crawled': 0,
            'start_time': datetime.utcnow(),
            'content_types': {},
            'status_codes': {},
            'errors': [],
            'max_pages': 0
        }

    @classmethod
    def from_crawler(cls, crawler):
        """Create pipeline instance from crawler settings"""
        return cls(
            mongodb_uri=crawler.settings.get('MONGODB_URI', 'mongodb://localhost:27017/crawler')
        )

    def open_spider(self, spider):
        """Initialize MongoDB connection and stats when spider opens"""
        try:
            # Initialize MongoDB connection
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.crawl_jobs = self.mongo_client.crawler[MONGO_CRAWL_JOB_COLLECTION]

            # Initialize stats
            # Get domain from spider.domain or extract from URL for URLSpider
            if hasattr(spider, 'domain'):
                self.stats['domain'] = spider.domain
            elif hasattr(spider, 'start_urls') and spider.start_urls:
                self.stats['domain'] = urlparse(spider.start_urls[0]).netloc
            else:
                self.stats['domain'] = 'unknown'

            self.stats['max_pages'] = getattr(spider, 'max_pages', 0)
            self.stats['crawl_id'] = getattr(spider, 'crawl_id', None)

            if not self.stats['crawl_id']:
                logger.error("crawl_id not available for stats pipeline. Stats will not be updated.")

            logger.info(f"StatsPipeline initialized for domain: {self.stats['domain']}")
        except Exception as e:
            logger.error(f"Failed to initialize StatsPipeline: {str(e)}")
            raise

    def process_item(self, item: Dict[str, Any], spider: Spider) -> Dict[str, Any]:
        """Update stats for each processed item"""
        try:
            self.stats['pages_crawled'] += 1

            # Get content type
            content_type = self._get_content_type(item)
            if content_type:
                self.stats['content_types'][content_type] = self.stats['content_types'].get(content_type, 0) + 1

            # Get status code
            status = item.get('status')
            if status:
                self.stats['status_codes'][str(status)] = self.stats['status_codes'].get(str(status), 0) + 1

            self._update_stats(spider)
        except Exception as e:
            logger.error(f"Error processing item stats: {str(e)}")
            self.stats['errors'].append(str(e))

        return item

    def close_spider(self, spider):
        """Update final stats when spider closes"""
        try:
            self.stats['end_time'] = datetime.utcnow()
            self._update_stats(spider)

            # Check if crawl_id is available and pages_crawled is zero
            crawl_id = self.stats.get('crawl_id')
            pages_crawled = self.stats.get('pages_crawled', 0)
            crawl_errors = []
            if crawl_id and pages_crawled == 0:
                crawl_errors.append('Crawl completed with zero pages crawled.')

            # Add any errors collected during the crawl
            if self.stats.get('errors'):
                crawl_errors.extend(self.stats['errors'])

            # Log crawl errors to MongoDB if any
            if crawl_id:
                if self.crawl_jobs is not None:
                    # Set crawl_status to 'completed' or 'failed' based on errors
                    crawl_status = 'failed' if crawl_errors else 'completed'
                    self.crawl_jobs.update_one(
                        {'crawl_id': crawl_id},
                        {'$set': {
                            'crawl_status': crawl_status,
                            'crawl_errors': crawl_errors,
                            'end_time': self.stats['end_time'],
                            'crawl_stats': self.stats
                        }},
                        upsert=False
                    )
                    logger.info(f"Set crawl_status to '{crawl_status}' for crawl_id {crawl_id}")
                    if crawl_errors:
                        logger.error(f"Crawl errors for {crawl_id}: {crawl_errors}")

            # Close MongoDB connection
            if self.mongo_client is not None:
                self.mongo_client.close()
        except Exception as e:
            logger.error(f"Error closing spider stats: {str(e)}")

    def _update_stats(self, spider: Spider) -> None:
        """Update crawl statistics in MongoDB"""
        try:
            if self.crawl_jobs is None:
                logger.error("Stats collection not initialized")
                return

            if not self.stats['crawl_id']:
                logger.error("crawl_id not available for stats update")
                return

            # Get current stats
            stats = spider.crawler.stats.get_stats()
            pages_crawled = int(stats.get('pages_crawled', 0))
            max_pages = int(stats.get('max_pages', 0))

            # Calculate completion percentage safely
            completion_percentage = 0
            if max_pages > 0:
                completion_percentage = round((pages_crawled / max_pages) * 100, 2)

            # Prepare update data, always set all expected fields
            self.stats['pages_crawled'] = pages_crawled
            self.stats['max_pages'] = max_pages
            self.stats['completion_percentage'] = completion_percentage
            self.stats['status_codes'] = stats.get('status_codes', {})
            self.stats['avg_response_time'] = stats.get('avg_response_time', 0)
            self.stats['success_rate'] = stats.get('success_rate', 0)
            self.stats['last_updated'] = datetime.utcnow()

            # Ensure all expected fields are present
            for key, default in [
                ('status_codes', {}),
                ('completion_percentage', 0),
                ('avg_response_time', 0),
                ('success_rate', 0),
            ]:
                if key not in self.stats:
                    self.stats[key] = default

            # Add js_rendering_domains if present on the spider
            if hasattr(spider, 'js_rendering_domains'):
                self.stats['js_rendering_domains'] = sorted(list(spider.js_rendering_domains))

            # Update MongoDB using crawl_id as the key
            self.crawl_jobs.update_one(
                {'crawl_id': self.stats['crawl_id']},
                {'$set': {'crawl_stats': self.stats}},
                upsert=False
            )
            logger.info(f"Updated stats for crawl_id {self.stats['crawl_id']}: {pages_crawled}/{max_pages} pages ({completion_percentage}%)")

        except Exception as e:
            logger.error(f"Error updating stats: {str(e)}")
            # Don't re-raise to avoid breaking the pipeline

    def _get_content_type(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract content type from response headers"""
        try:
            headers = item.get('headers', {})
            if not headers:
                return None

            content_type_header = headers.get(b'Content-Type', [b''])[0]
            if isinstance(content_type_header, list):
                content_type_header = content_type_header[0] if content_type_header else b''

            content_type = content_type_header.decode('utf-8', errors='ignore').split(';')[0].strip()
            return content_type
        except Exception as e:
            logger.error(f"Error getting content type: {str(e)}")
            return None