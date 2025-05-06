#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spider for crawling single URLs without following links.
"""

import logging
from typing import Iterator, Dict, Any, Optional
from urllib.parse import urlparse

from scrapy.http import Request, Response
from scrapy.exceptions import CloseSpider

from .base_spider import BaseSpider
from ..utils.url_utils import has_skipped_extension

logger = logging.getLogger(__name__)

class URLSpider(BaseSpider):
    """
    Spider for crawling single URLs without following links.

    This spider:
    1. Makes a single request to the target URL
    2. Does not follow any links
    3. Uses proxy and JS rendering fallback if needed
    """

    name = 'url_spider'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Create spider instance from crawler with settings."""
        spider = super().from_crawler(crawler, *args, **kwargs)

        # Get spider-specific settings
        settings = crawler.settings.get('SPIDER_SPECIFIC_SETTINGS', {}).get(cls.name, {})

        # Initialize spider attributes from settings
        spider.max_retries = settings.get('MAX_RETRIES', 3)
        spider.retry_delay = settings.get('RETRY_DELAY', 5)
        spider.backoff_factor = settings.get('BACKOFF_FACTOR', 2)

        return spider

    def __init__(
        self,
        url: str,
        domain: str = None,
        job_id: str = None,
        crawl_id: str = None,
        max_pages: int = 1,
        *args,
        **kwargs
    ):
        """
        Initialize the URL spider.

        Args:
            url: Target URL to crawl
            domain: Domain of the URL (if not provided, will be extracted from URL)
            job_id: Unique identifier for this crawl job (deprecated, use crawl_id)
            crawl_id: Unique identifier for this crawl job
            max_pages: Maximum number of pages to crawl (default: 1, ignored for URLSpider)
        """
        # Always set max_pages=1 for URL spider
        super().__init__(job_id=job_id, crawl_id=crawl_id, max_pages=1, *args, **kwargs)
        self.start_urls = [url]

        # Extract domain from URL if not provided
        if domain:
            self.domain = domain
        else:
            parsed_url = urlparse(url)
            self.domain = parsed_url.netloc

        logger.info(f"Initializing URLSpider for domain: {self.domain}, URL: {url}")

        # Initialize tracking variables
        self.crawled_urls = set()
        self.unique_pages_crawled = 0

    def start_requests(self) -> Iterator[Request]:
        """Generate initial request for the target URL."""
        target_url = self.start_urls[0]
        logger.info(f"Starting crawl of single URL: {target_url} (domain: {self.domain})")

        # Check if URL has skipped extension before proceeding
        if has_skipped_extension(target_url):
            logger.warning(f"URL has skipped extension, not crawling: {target_url}")
            # Track in stats
            self.stats['pages_skipped'] += 1
            if target_url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(target_url)

            # Update crawler stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_skipped')
            return

        # Start with direct crawl - no proxy, no JS rendering
        yield self.make_request(
            url=target_url,
            callback=self.parse,
            errback=self.handle_error,
            dont_filter=True,
            meta={
                'max_retries': getattr(self, 'max_retries', 3),
                'retry_count': 0,
                'url_level': 0,  # Single URL is at level 0
                'is_target_url': True,
                'domain': self.domain  # Add domain to meta for consistency
            }
        )

    def parse(self, response: Response) -> Iterator[Dict[str, Any]]:
        """Parse the response and yield the page item."""
        url = response.url

        # Add to crawled URLs and update stats
        if url not in self.crawled_urls:
            self.crawled_urls.add(url)
            self.unique_pages_crawled += 1

            # Update crawler stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_crawled')

            logger.info(f"Successfully crawled URL: {url} (status: {response.status})")

        # Ensure output includes domain information
        output = {
            'url': response.url,
            'status': response.status,
            'html': response.text,
            'headers': dict(response.headers),
            'page_type': 'html',
            'job_id': self.job_id,
            'domain': self.domain  # Explicitly include domain in output
        }

        # Yield the parsed output directly to maintain consistency with DomainSpider
        yield output

        # Close spider after processing the single URL
        raise CloseSpider('Finished crawling target URL')

    def handle_error(self, failure):
        """
        Handle failed requests with more robust fallback strategy.
        Similar to DomainSpider's approach but simplified for single URL.
        """
        url = failure.request.url
        error_reason = str(failure.value)

        # Update stats
        self.stats['pages_failed'] += 1
        logger.error(f"Failed to fetch {url}: {error_reason}")

        # Update crawler stats
        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            self.crawler.stats.inc_value('pages_failed')

        # Extract retry information
        retry_count = failure.request.meta.get('retry_count', 0)
        max_retries = failure.request.meta.get('max_retries', self.max_retries)

        # Determine if we should retry
        if retry_count < max_retries:
            # Create new request with increased retry count
            request = failure.request.copy()
            request.meta['retry_count'] = retry_count + 1
            request.dont_filter = True

            # Calculate exponential backoff delay
            delay = self.retry_delay * (self.backoff_factor ** retry_count)

            # First try with a proxy if not already using one
            if not request.meta.get('use_proxy'):
                logger.info(f"Retrying {url} with proxy (retry {retry_count + 1}/{max_retries})")
                request.meta['use_proxy'] = True
                request.meta['proxy_retry'] = True
                return request

            # Then try with JS rendering if not already using it
            elif not request.meta.get('js_render'):
                logger.info(f"Retrying {url} with JS rendering (retry {retry_count + 1}/{max_retries})")
                request.meta['js_render'] = True
                request.meta['js_retry'] = True
                return request

            # Finally, try with both proxy and JS rendering
            elif not (request.meta.get('proxy_retry') and request.meta.get('js_retry')):
                logger.info(f"Retrying {url} with proxy AND JS rendering (retry {retry_count + 1}/{max_retries})")
                request.meta['use_proxy'] = True
                request.meta['js_render'] = True
                request.meta['proxy_retry'] = True
                request.meta['js_retry'] = True
                return request

        # If we've exhausted all retries, log a final failure
        logger.error(f"Exhausted all retry attempts for {url}")

        # Call parent error handler as a fallback
        return super().handle_error(failure)