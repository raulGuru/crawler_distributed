#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
RetryMiddleware for Scrapy.

This middleware implements advanced retry logic with exponential backoff
for various failure types. It coordinates with the proxy middleware to
handle proxy and non-proxy retries appropriately.
"""

import logging
import time
import random
from urllib.parse import urlparse
from typing import Optional

from scrapy import signals
from scrapy.exceptions import NotConfigured, IgnoreRequest
from scrapy.utils.response import response_status_message
from twisted.internet import defer
from twisted.internet.error import (
    TimeoutError, DNSLookupError, ConnectionRefusedError, ConnectionDone,
    ConnectError, ConnectionLost, TCPTimedOutError
)
from scrapy.http import Request, Response
from scrapy.spiders import Spider
from lib.utils.proxy_manager import get_proxy_manager

logger = logging.getLogger(__name__)

# Retry on these network exceptions
NETWORK_EXCEPTIONS = (
    defer.TimeoutError,
    TimeoutError,
    DNSLookupError,
    ConnectionRefusedError,
    ConnectionDone,
    ConnectError,
    ConnectionLost,
    TCPTimedOutError,
    IOError,
    EOFError,
)

# Retry on these HTTP status codes
RETRY_HTTP_CODES = {
    500: {'priority': 'high', 'backoff_factor': 1.5, 'max_retries': 3},
    502: {'priority': 'high', 'backoff_factor': 1.5, 'max_retries': 3},
    503: {'priority': 'high', 'backoff_factor': 2.0, 'max_retries': 3},
    504: {'priority': 'high', 'backoff_factor': 1.5, 'max_retries': 3},
    408: {'priority': 'high', 'backoff_factor': 1.2, 'max_retries': 3},
    429: {'priority': 'critical', 'backoff_factor': 3.0, 'max_retries': 2},
    403: {'priority': 'low', 'backoff_factor': 2.0, 'max_retries': 2},
}


class RetryMiddleware:
    """
    Middleware for advanced retry logic with exponential backoff.

    This middleware improves on Scrapy's default retry mechanism
    by implementing differentiated retry policies based on error type
    and domain-specific behavior.
    """

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create middleware from crawler.

        Args:
            crawler: Scrapy crawler

        Returns:
            RetryMiddleware instance
        """
        # Get settings
        retry_enabled = crawler.settings.getbool('RETRY_ENABLED', True)
        if not retry_enabled:
            raise NotConfigured("RetryMiddleware is disabled (RETRY_ENABLED=False)")

        max_retries = crawler.settings.getint('RETRY_TIMES', 3)
        retry_delay = crawler.settings.getfloat('RETRY_DELAY', 5.0)
        retry_backoff_factor = crawler.settings.getfloat('RETRY_BACKOFF_FACTOR', 2.0)
        retry_jitter = crawler.settings.getbool('RETRY_JITTER', True)

        # Create middleware instance
        middleware = cls(
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff_factor=retry_backoff_factor,
            retry_jitter=retry_jitter,
        )

        # Store the crawler instance for use in method implementations
        middleware.crawler = crawler

        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def __init__(self, max_retries: int = 3, retry_delay: float = 5.0, retry_backoff_factor: float = 2.0, retry_jitter: bool = True):
        """
        Initialize the middleware.

        Args:
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Base delay between retries in seconds
            retry_backoff_factor (float): Multiplier for exponential backoff
            retry_jitter (bool): Whether to add random jitter to delay times
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_jitter = retry_jitter

        # Domain-specific retry tracking
        self.domain_retry_counts = {}

        # Statistics
        self.stats = {
            'retry_count': 0,
            'network_retries': 0,
            'status_retries': 0,
            'retry_success': 0,
            'retry_domains': set(),
        }

        logger.info(f"RetryMiddleware initialized (max_retries={max_retries}, "
                   f"retry_delay={retry_delay}, backoff_factor={retry_backoff_factor})")

    def spider_opened(self, spider: Spider):
        """
        Called when spider is opened.

        Args:
            spider: Scrapy spider
        """
        spider.crawler.stats.set_value('retry/count', 0)
        spider.crawler.stats.set_value('retry/max_reached', 0)

        # Set default retry values on spider for easier access
        spider.max_retries = self.max_retries

    def spider_closed(self, spider):
        """
        Called when spider is closed.

        Args:
            spider: Scrapy spider
        """
        # Log domains with most retries
        if self.domain_retry_counts:
            top_retry_domains = sorted(
                self.domain_retry_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]

            logger.info(f"Top domains by retry count: {top_retry_domains}")

    def process_response(self, request: Request, response: Response, spider: Spider) -> Response:
        """
        Process response to handle retryable status codes.

        Args:
            request: Scrapy Request
            response: Scrapy Response
            spider: Scrapy spider

        Returns:
            Response: Original response if not retrying
            Request: New request if retrying
        """
        # Skip if retry is disabled for this request
        if request.meta.get('dont_retry', False):
            return response

        # Check for retryable status codes
        status = response.status
        if status in RETRY_HTTP_CODES:
            # Get retry info for this request
            retry_config = RETRY_HTTP_CODES[status]
            retries = request.meta.get('retry_times', 0)
            max_retries_for_status = retry_config.get('max_retries', self.max_retries)

            # Get domain for tracking
            domain = self._get_domain(request.url)

            # For 403 errors, try to use proxy if available
            if status == 403:
                logger.info(f"Retry {retries+1} for {domain} (status 403): {request.url}")

                # Import proxy manager here to avoid circular import
                try:
                    proxy_manager = get_proxy_manager()

                    # Mark this domain as requiring a proxy by setting a flag in request.meta
                    if proxy_manager and not request.meta.get('proxy'):
                        logger.info(f"Marking domain {domain} as requiring proxy due to 403 error")
                        request.meta['force_proxy'] = True  # ProxyMiddleware will handle this

                        # Don't try to modify immutable settings
                        # Instead just use the proxy directly for this request
                        proxy = proxy_manager.get_proxy_for_domain(domain) if hasattr(proxy_manager, 'get_proxy_for_domain') else proxy_manager.get_proxy()
                        if proxy:
                            logger.info(f"Using proxy {proxy} for domain {domain} after 403 error")
                            request.meta['proxy'] = proxy
                except Exception as e:
                    logger.error(f"Error setting up proxy after 403: {e}")

            if retries < max_retries_for_status:
                # Update stats
                self.stats['retry_count'] += 1
                self.stats['status_retries'] += 1
                self.stats['retry_domains'].add(domain)

                spider.crawler.stats.inc_value('retry/count')
                spider.crawler.stats.inc_value('retry/status_retries')

                # Update domain retry count
                self.domain_retry_counts[domain] = self.domain_retry_counts.get(domain, 0) + 1

                # Calculate retry delay with exponential backoff
                backoff_factor = retry_config.get('backoff_factor', self.retry_backoff_factor)
                delay = self._calculate_delay(retries, self.retry_delay, backoff_factor)

                # Log the retry
                logger.debug(f"Retrying {request.url} (failed with status {status}, "
                            f"retry {retries+1}/{max_retries_for_status}) in {delay:.1f} seconds")

                # Create new request with incremented retry counter
                return self._retry_request(request, response, spider, reason=f"status {status}", delay=delay)

        return response

    def process_exception(self, request, exception, spider):
        """
        Process exception to handle network errors.

        Args:
            request: Scrapy Request
            exception: Exception that occurred
            spider: Scrapy spider

        Returns:
            None: If not retrying
            Request: New request if retrying
        """
        # Skip if retry is disabled for this request
        if request.meta.get('dont_retry', False):
            return None

        # Check if exception is retryable
        if isinstance(exception, NETWORK_EXCEPTIONS):
            # Get retry info for this request
            retries = request.meta.get('retry_times', 0)

            # Get domain for tracking
            domain = self._get_domain(request.url)

            if retries < self.max_retries:
                # Update stats
                self.stats['retry_count'] += 1
                self.stats['network_retries'] += 1
                self.stats['retry_domains'].add(domain)

                spider.crawler.stats.inc_value('retry/count')
                spider.crawler.stats.inc_value('retry/network_retries')

                # Update domain retry count
                self.domain_retry_counts[domain] = self.domain_retry_counts.get(domain, 0) + 1

                # Calculate retry delay with exponential backoff
                # Network errors get more aggressive backoff
                delay = self._calculate_delay(retries, self.retry_delay, self.retry_backoff_factor)

                # Log the retry
                logger.debug(f"Retrying {request.url} (failed with {exception.__class__.__name__}, "
                            f"retry {retries+1}/{self.max_retries}) in {delay:.1f} seconds")

                # Create new request with incremented retry counter
                return self._retry_request(request, reason=f"exception {exception.__class__.__name__}",
                                         spider=spider, delay=delay)

        # Not a retryable exception
        return None

    def _retry_request(self, request, response=None, spider=None, reason='unspecified', delay=None):
        """
        Create and schedule a new request for retry.

        Args:
            request: Original Scrapy Request
            response: Original Scrapy Response (optional)
            spider: Scrapy spider
            reason: Reason for the retry
            delay: Delay before retrying

        Returns:
            Request: New request for retry
        """
        # Get the number of retries so far
        retries = request.meta.get('retry_times', 0) + 1

        # Create a copy of the original request
        new_request = request.copy()
        new_request.meta['retry_times'] = retries
        new_request.meta['retry_reason'] = reason
        new_request.meta['retry_timestamp'] = time.time()

        # Don't filter duplicate request
        new_request.dont_filter = True

        # Remove any download timeout
        if 'download_timeout' in new_request.meta:
            # Increase timeout for retry attempts
            original_timeout = request.meta['download_timeout']
            new_request.meta['download_timeout'] = original_timeout * 1.5

        # Apply a delay if specified
        if delay:
            new_request.meta['retry_delay'] = delay

        # Log retry details
        domain = self._get_domain(request.url)
        logger.info(f"Retry {retries} for {domain} ({reason}): {request.url}")

        return new_request

    def _calculate_delay(self, retry_count, base_delay, backoff_factor):
        """
        Calculate delay time with exponential backoff and optional jitter.

        Args:
            retry_count (int): Current retry attempt number
            base_delay (float): Base delay in seconds
            backoff_factor (float): Multiplier for backoff

        Returns:
            float: Calculated delay in seconds
        """
        # Calculate exponential backoff
        delay = base_delay * (backoff_factor ** retry_count)

        # Add jitter if enabled (±20%)
        if self.retry_jitter:
            jitter = random.uniform(0.8, 1.2)
            delay *= jitter

        return delay

    def _get_domain(self, url):
        """
        Extract domain from URL.

        Args:
            url (str): URL to process

        Returns:
            str: Domain name
        """
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except Exception:
            return ''

    def _inc_stat(self, spider, key, amount=1):
        """Increment a stat, using Scrapy's stats collector if available, else dict."""
        if hasattr(spider, 'crawler') and hasattr(spider.crawler, 'stats'):
            spider.crawler.stats.inc_value(key, amount)
        else:
            self.stats[key] = self.stats.get(key, 0) + amount