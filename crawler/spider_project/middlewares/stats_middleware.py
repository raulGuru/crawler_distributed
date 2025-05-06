#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
StatsMiddleware for Scrapy.

This middleware collects and reports detailed crawling statistics,
including domain-specific metrics, resource types, and error rates.
"""

import logging
import time
from collections import defaultdict
from urllib.parse import urlparse
from scrapy import signals
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class StatsMiddleware:
    """
    Middleware for collecting detailed crawling statistics.

    This middleware tracks various metrics during the crawl, including:
    - Response time per domain
    - HTTP status code distribution
    - Content types and sizes
    - Error rates
    - Success/failure rates by domain

    The stats are both reported to the crawler stats collection and
    logged periodically for monitoring.
    """

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create middleware from crawler.

        Args:
            crawler: Scrapy crawler

        Returns:
            StatsMiddleware instance
        """
        # Get settings
        stats_enabled = crawler.settings.getbool('STATS_ENABLED', True)
        if not stats_enabled:
            raise NotConfigured("StatsMiddleware is disabled (STATS_ENABLED=False)")

        stats_dump = crawler.settings.getbool('STATS_DUMP', True)
        stats_interval = crawler.settings.getint('STATS_DUMP_INTERVAL', 60)  # seconds

        # Create middleware instance
        middleware = cls(
            stats_dump=stats_dump,
            stats_interval=stats_interval,
        )

        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        # If stats dump is enabled, connect to spider_idle signal to dump stats periodically
        if stats_dump:
            crawler.signals.connect(middleware.spider_idle, signal=signals.spider_idle)

        return middleware

    def __init__(self, stats_dump=True, stats_interval=60):
        """
        Initialize the middleware.

        Args:
            stats_dump (bool): Whether to periodically dump stats
            stats_interval (int): Interval between stats dumps in seconds
        """
        self.stats_dump = stats_dump
        self.stats_interval = stats_interval

        # Internal stats tracking
        self.domain_stats = defaultdict(lambda: {
            'requests': 0,
            'success': 0,
            'failure': 0,
            'total_bytes': 0,
            'total_time': 0,
            'status_counts': defaultdict(int),
        })

        self.content_type_stats = defaultdict(int)
        self.status_code_stats = defaultdict(int)
        self.last_dump_time = time.time()
        self.start_time = None

        logger.info(f"StatsMiddleware initialized (stats_dump={stats_dump}, "
                   f"interval={stats_interval}s)")

    def spider_opened(self, spider):
        """
        Called when spider is opened.

        Args:
            spider: Scrapy spider
        """
        self.start_time = time.time()
        self.last_dump_time = self.start_time

        # Initialize global stats
        spider.crawler.stats.set_value('stats/domains_crawled', 0)
        spider.crawler.stats.set_value('stats/total_response_time', 0)
        spider.crawler.stats.set_value('stats/total_bytes_received', 0)

    def spider_closed(self, spider):
        """
        Called when spider is closed.

        Args:
            spider: Scrapy spider
        """
        # Calculate elapsed time
        elapsed = time.time() - self.start_time

        # Get the number of pages successfully crawled
        success_count = sum(domain['success'] for domain in self.domain_stats.values())

        # Calculate average response time
        total_time = sum(domain['total_time'] for domain in self.domain_stats.values())
        avg_time = total_time / success_count if success_count else 0

        # Log final stats summary
        logger.info(f"Crawl completed in {elapsed:.2f}s")
        logger.info(f"Domains crawled: {len(self.domain_stats)}")
        logger.info(f"Pages crawled: {success_count}")
        logger.info(f"Average response time: {avg_time:.3f}s")

        # Dump detailed stats if enabled
        if self.stats_dump:
            self._dump_detailed_stats(spider)

    def spider_idle(self, spider):
        """
        Called when spider is idle.

        This is a good time to periodically dump stats.

        Args:
            spider: Scrapy spider
        """
        # Check if it's time to dump stats
        current_time = time.time()
        if current_time - self.last_dump_time >= self.stats_interval:
            self._dump_detailed_stats(spider)
            self.last_dump_time = current_time

    def process_request(self, request, spider):
        """
        Process outgoing request for stats tracking.

        Args:
            request: Scrapy Request
            spider: Scrapy spider

        Returns:
            None: Continue processing the request
        """
        # Add timestamp to measure response time
        request.meta['stats_start_time'] = time.time()

        # Continue processing the request
        return None

    def process_response(self, request, response, spider):
        """
        Process response to collect stats.

        Args:
            request: Scrapy Request
            response: Scrapy Response
            spider: Scrapy spider

        Returns:
            Response: Original response
        """
        # Calculate response time
        start_time = request.meta.get('stats_start_time')
        if start_time:
            response_time = time.time() - start_time
        else:
            response_time = 0

        # Get domain and status
        domain = self._get_domain(response.url)
        status = response.status

        # Get content type and size
        content_type = self._get_content_type(response)
        content_length = len(response.body)

        # Update domain stats
        self.domain_stats[domain]['requests'] += 1
        self.domain_stats[domain]['status_counts'][status] += 1

        # For successful responses (2xx)
        if 200 <= status < 300:
            self.domain_stats[domain]['success'] += 1
            self.domain_stats[domain]['total_bytes'] += content_length
            self.domain_stats[domain]['total_time'] += response_time
        else:
            self.domain_stats[domain]['failure'] += 1

        # Update global stats
        self.status_code_stats[status] += 1
        if content_type:
            self.content_type_stats[content_type] += 1

        # Update the spider's stats collector
        spider.crawler.stats.inc_value('stats/domains_crawled',
                                      spider.crawler.stats.get_value('stats/domains_crawled', 0) == 0)
        spider.crawler.stats.inc_value(f'stats/status_codes/{status}')
        spider.crawler.stats.inc_value('stats/total_response_time', response_time)
        spider.crawler.stats.inc_value('stats/total_bytes_received', content_length)

        # Return original response
        return response

    def process_exception(self, request, exception, spider):
        """
        Process exception for stats tracking.

        Args:
            request: Scrapy Request
            exception: Exception that occurred
            spider: Scrapy spider

        Returns:
            None: Continue processing the exception
        """
        # Get domain
        domain = self._get_domain(request.url)

        # Update domain stats
        self.domain_stats[domain]['requests'] += 1
        self.domain_stats[domain]['failure'] += 1

        # Track exception type
        exception_name = exception.__class__.__name__
        spider.crawler.stats.inc_value(f'stats/exceptions/{exception_name}')

        # Continue processing the exception
        return None

    def _dump_detailed_stats(self, spider):
        """
        Dump detailed stats to logs and update crawler stats.

        Args:
            spider: Scrapy spider
        """
        # Calculate elapsed time
        elapsed = time.time() - self.start_time

        # Get request counts
        total_requests = sum(domain['requests'] for domain in self.domain_stats.values())
        success_count = sum(domain['success'] for domain in self.domain_stats.values())
        failure_count = sum(domain['failure'] for domain in self.domain_stats.values())

        # Skip if no requests have been made
        if total_requests == 0:
            return

        # Calculate success rate
        success_rate = (success_count / total_requests) * 100 if total_requests else 0

        # Calculate average response time
        total_time = sum(domain['total_time'] for domain in self.domain_stats.values())
        avg_time = total_time / success_count if success_count else 0

        # Calculate requests per second
        rps = total_requests / elapsed if elapsed > 0 else 0

        # Log summary
        logger.info(f"--- Stats Summary (after {elapsed:.1f}s) ---")
        logger.info(f"Domains crawled: {len(self.domain_stats)}")
        logger.info(f"Total requests: {total_requests}")
        logger.info(f"Success: {success_count} ({success_rate:.1f}%)")
        logger.info(f"Failures: {failure_count}")
        logger.info(f"Average response time: {avg_time:.3f}s")
        logger.info(f"Requests per second: {rps:.2f}")

        # Get top domains by request count
        top_domains = sorted(
            self.domain_stats.items(),
            key=lambda x: x[1]['requests'],
            reverse=True
        )[:5]

        if top_domains:
            logger.info("Top domains by request count:")
            for domain, stats in top_domains:
                success_rate = (stats['success'] / stats['requests']) * 100 if stats['requests'] else 0
                logger.info(f"  {domain}: {stats['requests']} requests, {success_rate:.1f}% success")

        # Get status code distribution
        status_codes = sorted(self.status_code_stats.items())
        if status_codes:
            status_str = ", ".join(f"{status}: {count}" for status, count in status_codes)
            logger.info(f"Status code distribution: {status_str}")

        # Update crawler stats collector
        spider.crawler.stats.set_value('stats/success_rate', success_rate)
        spider.crawler.stats.set_value('stats/avg_response_time', avg_time)
        spider.crawler.stats.set_value('stats/requests_per_second', rps)

        html_saved_count = spider.crawler.stats.get_value('html_saved_count', 0)
        logger.info(f"Total HTML files saved: {html_saved_count}")

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

    def _get_content_type(self, response):
        """
        Extract content type from response.

        Args:
            response: Scrapy Response

        Returns:
            str: Content-Type or None if not found
        """
        content_type = response.headers.get('Content-Type', b'').decode('utf-8', 'ignore')

        # Extract the main content type (before parameters like charset)
        if content_type:
            # Split by semicolon and get the first part (main type)
            main_type = content_type.split(';')[0].strip()
            return main_type

        return None