"""
HTML storage pipeline that saves crawled pages to disk.
"""

from datetime import datetime
import os
import logging
from typing import Dict, Any
from urllib.parse import urlparse

from scrapy.exceptions import DropItem, CloseSpider
from crawler.spider_project.utils.url_utils import normalize_domain

logger = logging.getLogger(__name__)

class HTMLStoragePipeline:
    """
    Pipeline for storing HTML content to disk.

    This pipeline:
    1. Creates a directory structure based on domain
    2. Saves HTML content to files
    3. Ensures files are properly written before proceeding
    """

    def __init__(self, html_folder: str = None):
        """
        Initialize the pipeline.

        Args:
            html_folder: Base folder for storing HTML files
        """
        self.base_folder = html_folder or 'data/html'
        self.logger = logger

        # Ensure base folder exists with proper permissions
        try:
            os.makedirs(self.base_folder, mode=0o755, exist_ok=True)
            self.logger.info(f"HTML storage directory ready: {self.base_folder}")
        except Exception as e:
            self.logger.error(f"Failed to create HTML storage directory: {str(e)}")
            raise

    @classmethod
    def from_crawler(cls, crawler):
        """Create pipeline from crawler."""
        html_folder = crawler.settings.get('HTML_STORAGE_FOLDER', 'data/html')
        return cls(html_folder=html_folder)

    def process_item(self, item: Dict[str, Any], spider) -> Dict[str, Any]:
        """
        Process and store an item.

        Args:
            item: The scraped item containing HTML content
            spider: The spider that generated this item

        Returns:
            The processed item

        Raises:
            DropItem: If the item cannot be processed or saved
        """
        # Skip storage if skip_html_storage is set
        if item.get('skip_html_storage'):
            return item

        try:
            # Extract domain and create directory
            domain = item.get('domain') or urlparse(item['url']).netloc
            domain = normalize_domain(domain)
            domain_folder = os.path.join(self.base_folder, domain)

            # Create domain folder with proper permissions
            try:
                os.makedirs(domain_folder, mode=0o755, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create domain directory {domain_folder}: {str(e)}")
                raise DropItem(f"Failed to create domain directory: {str(e)}")

            # Create filename from URL
            url_path = urlparse(item['url']).path
            if not url_path or url_path == '/':
                filename = 'index.html'
            else:
                # Convert path to filename
                filename = url_path.strip('/').replace('/', '_')
                # Only add .html extension for HTML content
                if not filename.endswith(('.txt', '.xml')):
                    filename += '.html'

            # Full path for the file
            file_path = os.path.join(domain_folder, filename)

            # Save HTML content
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(item['html'])
                    # Ensure content is written to disk
                    f.flush()
                    os.fsync(f.fileno())

                self.logger.info(f"Successfully saved HTML for {item['url']} to {file_path}")

                # Add storage info to item
                item['storage'] = {
                    'file_path': file_path,
                    'domain_folder': domain_folder,
                    'filename': filename
                }
                item['html_file_path'] = file_path
                item['crawled_at'] = datetime.utcnow().isoformat()

                # --- Max pages enforcement logic & stats update ---
                if hasattr(spider, 'crawler') and hasattr(spider.crawler, 'stats'):
                    stats = spider.crawler.stats
                    if stats: # Explicit check
                        self.logger.debug(f"Stats collector found. Current pages_crawled: {stats.get_value('pages_crawled', 0)}")
                        pages_crawled = stats.get_value('pages_crawled', 0) + 1
                        stats.set_value('pages_crawled', pages_crawled)
                        self.logger.debug(f"Updated pages_crawled to: {pages_crawled}")

                        max_pages = getattr(spider, 'max_pages', 0)
                        if max_pages and pages_crawled >= max_pages:
                            self.logger.info(f"Reached max_pages limit ({max_pages}) after saving HTML. Stopping crawl.")
                            # raise CloseSpider(f"Reached max_pages limit: {max_pages}")

                        html_saved_count = stats.get_value('html_saved_count', 0) + 1
                        stats.set_value('html_saved_count', html_saved_count)
                        self.logger.debug(f"Updated html_saved_count to: {html_saved_count}")
                    else:
                        self.logger.warning(f"spider.crawler.stats is None for {item['url']}, cannot update page/save counts directly here.")
                else:
                    self.logger.warning(f"spider.crawler.stats not available for {item['url']}, cannot update page/save counts directly here.")

                return item

            except IOError as e:
                raise DropItem(f"Failed to save HTML file for {item['url']}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error processing item from {item.get('url', 'unknown URL')}: {str(e)}")
            raise DropItem(f"Error processing item: {str(e)}")

    def close_spider(self, spider):
        """
        Clean up when spider closes.
        """
        self.logger.info("HTML Storage Pipeline closed")