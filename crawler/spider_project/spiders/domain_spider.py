#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spider for crawling entire domains with support for sitemaps and BFS crawling.
"""

import logging
from typing import Iterator, Optional, Dict, Any, Generator, List, Set
from urllib.parse import urljoin, urlparse
from collections import deque
import re

from scrapy.http import Request, Response
from scrapy.spiders import CrawlSpider, Rule, Spider
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import CloseSpider
from scrapy.utils.response import get_base_url

from .base_spider import BaseSpider
from lib.utils.sitemap_utils import (
    get_urls_from_sitemap,
    locate_sitemap_url,
    is_sitemap_outdated,
    fetch_sitemap,
    is_sitemap_index,
    extract_urls_from_sitemap_index,
    extract_urls_from_sitemap,
    prioritize_urls
)
from ..utils.url_utils import has_skipped_extension

logger = logging.getLogger(__name__)

class DomainSpider(BaseSpider):
    """
    Spider for crawling entire domains with configurable strategies.

    This spider defaults to breadth-first search (BFS) crawling for more uniform coverage.
    1. If use_sitemap=True, it will use sitemaps to discover URLs.
    2. Otherwise, it performs a BFS crawl starting from the homepage, extracting links level by level.
    """

    name = 'domain_spider'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Create spider instance from crawler with settings."""
        spider = super().from_crawler(crawler, *args, **kwargs)

        # Get spider-specific settings
        settings = crawler.settings.get('SPIDER_SPECIFIC_SETTINGS', {}).get(cls.name, {})

        # Initialize spider attributes from settings
        spider.queue_size = settings.get('QUEUE_SIZE', 1000)
        spider.batch_size = settings.get('BATCH_SIZE', 10)
        spider.max_retries = settings.get('MAX_RETRIES', 3)
        spider.retry_delay = settings.get('RETRY_DELAY', 5)
        spider.backoff_factor = settings.get('BACKOFF_FACTOR', 2)
        spider.sitemap_max_age_days = settings.get('SITEMAP_MAX_AGE_DAYS', 90)
        spider.concurrent_requests = settings.get('CONCURRENT_REQUESTS', 4)
        spider.concurrent_requests_per_domain = settings.get('CONCURRENT_REQUESTS_PER_DOMAIN', 4)

        # Use effective max_pages for CLOSESPIDER_PAGECOUNT
        effective_max_pages = getattr(spider, 'max_pages', 50) + 2
        spider._effective_max_pages = effective_max_pages
        crawler.settings.set('CLOSESPIDER_PAGECOUNT', effective_max_pages, priority='spider')

        return spider

    def __init__(
        self,
        domain: str,
        job_id: str = None,
        crawl_id: str = None,
        max_pages: int = 50,
        use_sitemap: bool | str = False,
        start_urls: Optional[list] = None,
        *args,
        **kwargs
    ):
        """
        Initialize the domain spider.

        Args:
            domain: Target domain to crawl
            job_id: Unique identifier for this crawl job (deprecated, use crawl_id)
            crawl_id: Unique identifier for this crawl job
            max_pages: Maximum number of pages to crawl
            use_sitemap: Whether to try using sitemap for URL discovery. Default is False (BFS preferred).
            start_urls: Optional list of start URLs (defaults to domain homepage)
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        super().__init__(job_id=job_id, crawl_id=crawl_id, max_pages=max_pages, *args, **kwargs)

        self.domain = domain
        # Convert use_sitemap to boolean if it's a string
        if isinstance(use_sitemap, str):
            self.use_sitemap = use_sitemap.lower() in ('true', 'yes', '1')
        else:
            self.use_sitemap = bool(use_sitemap)
        self.sitemap_processed = False  # Flag to track if sitemap has been processed

        # Queue for managing URLs with bounded size
        self.url_queue = deque()
        self.currently_crawling = set()
        self.crawled_urls = set()  # Track unique URLs that have been crawled
        self.unique_pages_crawled = 0  # Track unique pages crawled
        self.enqueued_urls = set()  # Track URLs currently in the queue

        # Set allowed domains to restrict crawling
        self.allowed_domains = [domain]
        if domain.startswith('www.'):
            self.allowed_domains.append(domain[4:])  # Also allow non-www
        else:
            self.allowed_domains.append(f'www.{domain}')  # Also allow www

        # Set start URLs
        self.start_urls = start_urls or [f'http://{domain}', f'https://{domain}']

        # Configure link extractor for BFS crawling
        self.link_extractor = LinkExtractor(
            allow_domains=self.allowed_domains,
            deny_extensions=[
                # Exclude common non-HTML extensions
                'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
                'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg',
                'mp3', 'mp4', 'wav', 'avi', 'mov',
                'zip', 'rar', 'gz', 'tar',
            ],
            # Additional URL patterns to skip
            deny=(
                r'\?sort=',        # Skip sorting URLs
                r'\?page=\d+',     # Skip pagination
                r'\?filter=',      # Skip filter URLs
                r'/tag/',          # Skip tag pages
                r'/category/',     # Skip category listings
                r'/author/',       # Skip author pages
                r'/search/',       # Skip search results
                r'/feed/',         # Skip RSS feeds
                r'/rss/',          # Skip RSS feeds
                r'/print/',        # Skip print versions
                r'/amp/',          # Skip AMP pages
                r'/cdn-cgi/l/email-protection',  # Skip Cloudflare email protection URLs
            )
        )

        # Use effective max_pages for internal logic
        self._effective_max_pages = int(max_pages) + 2

    def _enqueue_url(self, url: str, callback=None, meta=None):
        """Add URL to queue for crawling if queue not full and max_pages not reached (soft check)"""
        # Soft check: do not enqueue if max_pages reached according to stats
        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            stats = self.crawler.stats
            pages_crawled = stats.get_value('pages_crawled', 0)
            max_pages = getattr(self, '_effective_max_pages', 0)
            if max_pages and pages_crawled >= max_pages:
                logger.info(f"[SOFT CHECK] Not enqueuing {url}: effective max_pages limit ({max_pages}) reached (pages_crawled={pages_crawled})")
                return
        # Check if URL has skipped extension before enqueuing
        if has_skipped_extension(url):
            logger.warning(f"URL has skipped extension, not enqueuing: {url}")
            # Track in stats
            self.stats['pages_skipped'] += 1
            if url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(url)

            # Update crawler stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_skipped')
            return

        # Continue with normal enqueuing for non-skipped URLs
        in_queue = url in self.enqueued_urls
        if not in_queue and url not in self.currently_crawling and url not in self.crawled_urls:
            if len(self.url_queue) < self.queue_size:
                self.url_queue.append({
                    'url': url,
                    'callback': callback or self.parse,
                    'meta': meta or {}
                })
                self.enqueued_urls.add(url)
            else:
                logger.warning(f"URL queue full ({self.queue_size} items), skipping: {url}")

    def _get_next_urls(self) -> Optional[Dict]:
        """Get next URLs to crawl from the queue."""
        if not self.url_queue:
            return None

        url_data = self.url_queue.popleft()
        if isinstance(url_data, str):
            url_data = {'url': url_data}

        url = url_data['url']
        self.enqueued_urls.discard(url)

        if url not in self.currently_crawling:
            self.currently_crawling.add(url)
            return url_data
        return None

    def _remove_from_crawling(self, url: str) -> None:
        """Safely remove URL from currently crawling set"""
        try:
            if url in self.currently_crawling:
                self.currently_crawling.remove(url)
        except KeyError:
            logger.warning(f"URL not found in currently_crawling set: {url}")

    def start_requests(self) -> Iterator[Request]:
        """
        Generate initial requests based on configuration.

        Always starts with direct crawl of homepage. If that fails,
        the error handler will switch to proxy/JS rendering as needed.
        """
        homepage_url = self.start_urls[0]  # Try first URL (usually https)
        self._enqueue_url(homepage_url, callback=self._handle_homepage, meta={'is_homepage': True})

        # Only attempt sitemap if explicitly enabled
        if self.use_sitemap:
            logger.info(f"Attempting to use sitemap for {self.domain}")
            # Try to locate sitemap via robots.txt or common locations
            sitemap_url = locate_sitemap_url(self.domain)
            if sitemap_url:
                logger.info(f"Sitemap found for {self.domain}: {sitemap_url}")
                # Process the discovered sitemap URL (index or regular)
                yield from self._process_sitemap_urls(sitemap_url)
                # After processing, enqueue any remaining URLs
                while True:
                    url_data = self._get_next_urls()
                    if not url_data:
                        break
                    yield self.make_request(
                        url=url_data['url'],
                        callback=url_data['callback'],
                        errback=self.handle_error,
                        dont_filter=True,
                        meta=url_data['meta']
                    )
                return  # Only fall back to BFS if sitemap discovery fails
            else:
                logger.warning(f"No sitemap found for {self.domain}, switching to BFS crawling.")

        # If not using sitemap or sitemap not found, start BFS crawling from homepage
        while True:
            url_data = self._get_next_urls()
            if not url_data:
                break
            yield self.make_request(
                url=url_data['url'],
                callback=url_data['callback'],
                errback=self.handle_error,
                dont_filter=True,
                meta=url_data['meta']
            )

    def _handle_homepage(self, response: Response) -> Generator[Request, None, None]:
        """Handle the homepage response and decide crawling strategy"""
        try:
            homepage_url = response.url
            if homepage_url in self.currently_crawling:
                self._remove_from_crawling(homepage_url)

            # Always yield the homepage response first
            yield from self._parse_page(response)

            # Only try sitemap if explicitly enabled and not processed yet
            if self.use_sitemap and not self.sitemap_processed:
                # First try robots.txt to find sitemap URLs
                robots_url = urljoin(homepage_url, '/robots.txt')
                if robots_url not in self.currently_crawling:
                    self._enqueue_url(
                        url=robots_url,
                        callback=self._parse_robots,
                        meta={'start_url': homepage_url}
                    )
                    yield self.make_request(
                        url=robots_url,
                        callback=self._parse_robots,
                        meta={'start_url': homepage_url}
                    )

            # Start BFS crawling if we haven't hit max pages yet
            if self.unique_pages_crawled < self._effective_max_pages:
                yield from self._start_bfs_crawl(response)

        except Exception as e:
            logger.error(f"Error in _handle_homepage for {response.url}: {str(e)}")
            # If homepage processing fails, try to continue with BFS crawl

            # Always try to continue with BFS crawl as fallback
            yield from self._start_bfs_crawl(response)

    def _process_sitemap_urls(self, sitemap_url: str) -> Iterator[Request]:
        """
        Process a sitemap URL, handling both regular sitemaps and sitemap indexes.

        Args:
            sitemap_url: URL of the sitemap to process

        Returns:
            Iterator of Requests for URLs found in the sitemap
        """
        # Skip if sitemap is outdated
        if is_sitemap_outdated(sitemap_url, self.sitemap_max_age_days):
            logger.info(f"Sitemap {sitemap_url} is outdated (older than {self.sitemap_max_age_days} days)")
            return

        # Fetch sitemap content
        sitemap_content = fetch_sitemap(sitemap_url)
        if not sitemap_content:
            logger.warning(f"Failed to fetch sitemap: {sitemap_url}")
            return

        # Check if it's a sitemap index
        if is_sitemap_index(sitemap_content):
            logger.info(f"Processing sitemap index: {sitemap_url}")
            # Extract child sitemap URLs
            child_sitemap_urls = extract_urls_from_sitemap_index(sitemap_content, sitemap_url)

            # Process each child sitemap
            all_urls = []
            for child_url in child_sitemap_urls:
                child_content = fetch_sitemap(child_url)
                if child_content:
                    urls = extract_urls_from_sitemap(child_content, child_url)
                    all_urls.extend(urls)

                # Stop if we have enough URLs
                if len(all_urls) >= self._effective_max_pages:
                    break

            # Prioritize and limit URLs
            if all_urls:
                prioritized_urls = prioritize_urls(all_urls, self._effective_max_pages)

                # Enqueue and yield requests for the prioritized URLs
                for url in prioritized_urls:
                    if url not in self.crawled_urls and self.unique_pages_crawled < self._effective_max_pages:
                        self._enqueue_url(url, callback=self._parse_page)
                        yield self.make_request(
                            url=url,
                            callback=self._parse_page,
                            errback=self.handle_error
                        )

                # Mark sitemap as processed
                self.sitemap_processed = True
                logger.info(f"Processed sitemap index with {len(prioritized_urls)} prioritized URLs")
        else:
            # Regular sitemap
            logger.info(f"Processing regular sitemap: {sitemap_url}")
            urls = extract_urls_from_sitemap(sitemap_content, sitemap_url)

            # Prioritize and limit URLs
            if urls:
                prioritized_urls = prioritize_urls(urls, self._effective_max_pages)

                # Enqueue and yield requests for the prioritized URLs
                for url in prioritized_urls:
                    if url not in self.crawled_urls and self.unique_pages_crawled < self._effective_max_pages:
                        self._enqueue_url(url, callback=self._parse_page)
                        yield self.make_request(
                            url=url,
                            callback=self._parse_page,
                            errback=self.handle_error
                        )

                # Mark sitemap as processed
                self.sitemap_processed = True
                logger.info(f"Processed regular sitemap with {len(prioritized_urls)} prioritized URLs")

    def _parse_sitemap(self, response: Response) -> Iterator[Request]:
        """
        Parse XML sitemap and generate requests for each URL.
        """
        try:
            # Skip HTML storage for sitemap.xml
            response.meta['skip_html_storage'] = True

            # Use the sitemap processing function
            yield from self._process_sitemap_urls(response.url)

            # If sitemap didn't provide any URLs or wasn't processed successfully,
            # fall back to BFS crawling
            if not self.sitemap_processed and self.unique_pages_crawled < self._effective_max_pages:
                logger.info(f"Sitemap for {self.domain} didn't provide URLs, falling back to BFS crawl")
                yield from self._start_bfs_crawl(response)

        except Exception as e:
            logger.error(f"Error parsing sitemap for {self.domain}: {str(e)}")
            if self.unique_pages_crawled < self._effective_max_pages:
                yield from self._start_bfs_crawl(response)

    def _parse_robots(self, response: Response) -> Iterator[Request]:
        """
        Parse robots.txt to find sitemap locations.
        """
        try:
            # Skip HTML storage for robots.txt
            response.meta['skip_html_storage'] = True

            # Check if we already found a sitemap
            if self.sitemap_processed:
                return

            sitemap_urls = []
            # Look for Sitemap: directives
            for line in response.text.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(sitemap_url)

            # Process found sitemap URLs
            if sitemap_urls:
                for sitemap_url in sitemap_urls:
                    yield from self._process_sitemap_urls(sitemap_url)
                    if self.sitemap_processed:
                        break

            # Fall back to common sitemap locations if none found in robots.txt
            if not self.sitemap_processed:
                sitemap_url = locate_sitemap_url(self.domain, response.text)
                if sitemap_url:
                    yield from self._process_sitemap_urls(sitemap_url)

            # If no sitemap URLs were processed, fall back to BFS crawl
            if not self.sitemap_processed and self.unique_pages_crawled < self._effective_max_pages:
                logger.info(f"No sitemaps found in robots.txt for {self.domain}, falling back to BFS crawl")
                start_url = response.meta.get('start_url')
                if start_url:
                    yield self.make_request(url=start_url, callback=self._start_bfs_crawl)

        except Exception as e:
            logger.error(f"Error parsing robots.txt for {self.domain}: {str(e)}")
            if self.unique_pages_crawled < self._effective_max_pages:
                start_url = response.meta.get('start_url')
                if start_url:
                    yield self.make_request(url=start_url, callback=self._start_bfs_crawl)

    def _handle_sitemap_error(self, failure) -> Iterator[Request]:
        """
        Handle errors in sitemap fetching by falling back to BFS crawl.
        """
        logger.warning(f"Failed to fetch sitemap for {self.domain}: {str(failure.value)}")
        start_url = failure.request.meta.get('start_url')
        if start_url and self.unique_pages_crawled < self._effective_max_pages:
            yield self.make_request(url=start_url, callback=self._start_bfs_crawl)

    def _start_bfs_crawl(self, response: Response) -> Iterator[Request]:
        """Start BFS crawling from a response."""
        logger.info(f"Starting BFS crawl from {response.url}")
        # Extract all links
        links = self.link_extractor.extract_links(response)
        # Log the number of links found for debugging
        logger.info(f"Found {len(links)} links on {response.url}")

        # Queue each link for crawling (soft check inside _enqueue_url)
        for link in links:
            self._enqueue_url(link.url)

        # Process queued URLs (soft check before yielding requests)
        while self.url_queue and self.unique_pages_crawled < self._effective_max_pages:
            # Soft check: do not yield if max_pages reached according to stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                stats = self.crawler.stats
                pages_crawled = stats.get_value('pages_crawled', 0)
                max_pages = getattr(self, '_effective_max_pages', 0)
                if max_pages and pages_crawled >= max_pages:
                    logger.info(f"[SOFT CHECK] Not yielding more requests: effective max_pages limit ({max_pages}) reached (pages_crawled={pages_crawled})")
                    break
            url_data = self._get_next_urls()
            if url_data:
                yield self.make_request(
                    url=url_data['url'],
                    callback=self._parse_page,
                    errback=self.handle_error,
                    meta=url_data.get('meta', {})
                )
                # Log for debugging
                logger.debug(f"Queuing URL for crawling: {url_data['url']}")

    def _parse_page(self, response: Response) -> Iterator[Dict[str, Any]]:
        """Parse a crawled page."""
        # Remove from currently crawling set
        url = response.url
        self._remove_from_crawling(url)

        # Add to crawled URLs
        if url not in self.crawled_urls:
            self.crawled_urls.add(url)
            self.unique_pages_crawled += 1
            logger.info(f"Crawled page {self.unique_pages_crawled}/{self._effective_max_pages}: {url}")

            # Only extract new links if we haven't reached the max pages limit
            if self.unique_pages_crawled < self._effective_max_pages:
                # Extract and queue new links
                links = self.link_extractor.extract_links(response)
                for link in links:
                    if link.url not in self.crawled_urls and link.url not in self.currently_crawling:
                        self._enqueue_url(link.url)

                # Process more URLs from the queue up to concurrent limit
                while self.url_queue and len(self.currently_crawling) < self.concurrent_requests_per_domain:
                    url_data = self._get_next_urls()
                    if url_data:
                        yield self.make_request(
                            url=url_data['url'],
                            callback=self._parse_page,
                            errback=self.handle_error,
                            meta=url_data.get('meta', {})
                        )
                    else:
                        break

        # Yield the parsed page data
        output = {
            'url': response.url,
            'status': response.status,
            'html': response.text,
            'headers': dict(response.headers),
            'page_type': 'html',
            'job_id': self.job_id,
            'crawl_id': self.crawl_id,
            'domain': self.domain,
            **self.custom_params
        }

        yield output

    def _is_sitemap_too_old(self, lastmod: str) -> bool:
        """
        Check if sitemap's lastmod date is older than specified max age days.
        """
        from datetime import datetime, timedelta
        try:
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"):
                try:
                    lastmod_date = datetime.strptime(lastmod, fmt)
                    return (datetime.now() - lastmod_date) > timedelta(days=self.sitemap_max_age_days)
                except ValueError:
                    continue
            return False  # If we can't parse the date, assume it's not too old
        except Exception as e:
            logger.warning(f"Error checking if sitemap is too old: {e}")
            return False

    def handle_error(self, failure):
        """Handle failed requests."""
        # Safely remove from currently crawling set
        if hasattr(failure, 'request') and hasattr(failure.request, 'url'):
            self._remove_from_crawling(failure.request.url)

        # Call parent's error handler
        result = super().handle_error(failure)

        # Process next URLs in queue if we got a retry request back
        if result and isinstance(result, Request):
            self._enqueue_url(
                url=result.url,
                callback=result.callback or self.parse,
                meta=result.meta or {}
            )

        # Process next URL in queue
        url_data = self._get_next_urls()
        if url_data:
            return self.make_request(
                url=url_data['url'],
                callback=url_data.get('callback', self._parse_page),
                errback=self.handle_error,
                dont_filter=True,
                meta=url_data.get('meta', {})
            )
