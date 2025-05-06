#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ContentFilterMiddleware for Scrapy.

This middleware filters out non-HTML content based on URL patterns
and content-type headers to avoid downloading unnecessary files.
"""

import logging
import os
import re
from urllib.parse import urlparse
from scrapy import signals
from scrapy.exceptions import NotConfigured, IgnoreRequest
from ..utils.url_utils import has_skipped_extension

logger = logging.getLogger(__name__)


class ContentFilterMiddleware:
    """
    Middleware to filter out non-HTML resources.

    This middleware prevents the crawler from downloading non-HTML resources
    like images, videos, documents, etc. by checking URL extensions and
    content-type headers.
    """

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create middleware from crawler.

        Args:
            crawler: Scrapy crawler

        Returns:
            ContentFilterMiddleware instance
        """
        # Get settings
        allowed_content_types = crawler.settings.getlist('ALLOWED_CONTENT_TYPES', [
            'text/html',
            'application/xhtml+xml',
            'application/xml',
            'text/xml',
            'text/plain',  # Allow for robots.txt and sitemaps
            'application/xml',  # Allow for sitemaps
            'application/x-xml',  # Allow for sitemaps
        ])

        skipped_extensions = crawler.settings.getlist('SKIPPED_EXTENSIONS', [
            # images
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico',
            # documents
            '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.csv',
            # archives
            '.zip', '.rar', '.gz', '.tar', '.7z',
            # audio/video
            '.mp3', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.wma', '.aac', '.ogg',
            # other
            '.css', '.js', '.json', '.rss', '.atom'  # Removed .xml to allow sitemaps
        ])

        # Create middleware instance
        middleware = cls(
            allowed_content_types=allowed_content_types,
            skipped_extensions=skipped_extensions,
        )

        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def __init__(self, allowed_content_types=None, skipped_extensions=None):
        """
        Initialize the middleware.

        Args:
            allowed_content_types (list): List of allowed content type prefixes
            skipped_extensions (list): List of file extensions to skip
        """
        self.allowed_content_types = allowed_content_types or []
        self.skipped_extensions = skipped_extensions or []

        # Statistics
        self.stats = {
            'url_filtered': 0,
            'content_type_filtered': 0,
            'allowed': 0,
        }

        logger.info(f"ContentFilterMiddleware initialized with {len(self.allowed_content_types)} "
                   f"allowed content types and {len(self.skipped_extensions)} skipped extensions")

    def spider_opened(self, spider):
        """
        Called when spider is opened.

        Args:
            spider: Scrapy spider
        """
        # Set up stats
        spider.crawler.stats.set_value('content_filter/url_filtered', 0)
        spider.crawler.stats.set_value('content_filter/content_type_filtered', 0)
        spider.crawler.stats.set_value('content_filter/allowed', 0)

    def spider_closed(self, spider):
        """
        Called when spider is closed.

        Args:
            spider: Scrapy spider
        """
        # Log final stats
        logger.info(f"ContentFilterMiddleware stats: {self.stats}")

    def process_request(self, request, spider):
        """
        Process outgoing request to filter by URL pattern.

        Args:
            request: Scrapy Request
            spider: Scrapy spider

        Returns:
            None: Continue processing the request
            IgnoreRequest: Skip the request
        """
        # Skip middleware checks if explicitly requested
        if request.meta.get('skip_content_filter', False):
            return None

        url = request.url

        # Check if URL has a skipped extension - use utility function
        if has_skipped_extension(url):
            # Update stats
            self.stats['url_filtered'] += 1
            spider.crawler.stats.inc_value('content_filter/url_filtered')

            # Update spider stats for skipped URLs if available
            if hasattr(spider, 'stats') and 'skipped_urls' in spider.stats:
                spider.stats['pages_skipped'] += 1
                if url not in spider.stats['skipped_urls']:
                    spider.stats['skipped_urls'].append(url)

            # Log the filtered URL
            logger.warning(f"URL has skipped extension: {url}")

            # Skip this request
            raise IgnoreRequest(f"URL has skipped extension: {url}")

        # Continue processing the request
        return None

    def process_response(self, request, response, spider):
        """
        Process response to filter by content-type.

        Args:
            request: Scrapy Request
            response: Scrapy Response
            spider: Scrapy spider

        Returns:
            Response: Continue processing the response
            IgnoreRequest: Skip the response
        """
        # Skip middleware checks if explicitly requested
        if request.meta.get('skip_content_filter', False):
            return response

        # Always allow robots.txt and sitemap.xml
        if request.url.endswith(('robots.txt', 'sitemap.xml')):
            self.stats['allowed'] += 1
            spider.crawler.stats.inc_value('content_filter/allowed')
            return response

        # Check URL extension - use utility function
        if has_skipped_extension(request.url):
            self.stats['url_filtered'] += 1
            spider.crawler.stats.inc_value('content_filter/url_filtered')

            # Update spider stats for skipped URLs if available
            if hasattr(spider, 'stats') and 'skipped_urls' in spider.stats:
                spider.stats['pages_skipped'] += 1
                if request.url not in spider.stats['skipped_urls']:
                    spider.stats['skipped_urls'].append(request.url)

            logger.warning(f"URL has skipped extension: {request.url}")
            raise IgnoreRequest(f"URL has skipped extension: {request.url}")

        # Get content type
        content_type = response.headers.get(b'Content-Type', b'').decode('utf-8').split(';')[0].strip().lower()

        # Check content type
        if content_type and content_type not in self.allowed_content_types:
            self.stats['content_type_filtered'] += 1
            spider.crawler.stats.inc_value('content_filter/content_type_filtered')
            logger.warning(f"Response has non-allowed content-type: {content_type}")
            raise IgnoreRequest(f"Response has non-allowed content-type: {content_type}")

        self.stats['allowed'] += 1
        spider.crawler.stats.inc_value('content_filter/allowed')
        return response

    def _get_content_type(self, response):
        """
        Extract content type from response.

        Args:
            response: Scrapy Response

        Returns:
            str: Content-Type lowercase or None if not found
        """
        content_type = response.headers.get('Content-Type', b'').decode('utf-8', 'ignore').lower()

        # Extract the main content type (before parameters like charset)
        if content_type:
            # Split by semicolon and get the first part (main type)
            main_type = content_type.split(';')[0].strip()
            return main_type

        return None

    def _is_allowed_content_type(self, content_type):
        """
        Check if content type is allowed.

        Args:
            content_type (str): Content-Type to check

        Returns:
            bool: True if content type is allowed
        """
        # If no allowed types specified, allow everything
        if not self.allowed_content_types:
            return True

        # Check if content_type matches any allowed type
        for allowed_type in self.allowed_content_types:
            if content_type.startswith(allowed_type):
                return True

        return False