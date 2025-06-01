#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Base spider class that implements common functionality for both URL and domain spiders.
"""

import logging
from typing import Dict, Any, Iterator
from urllib.parse import urlparse

from scrapy.http import Request, Response
from scrapy.spiders import Spider
from scrapy.exceptions import CloseSpider, IgnoreRequest
from scrapy.crawler import Crawler

from ..utils.url_utils import has_skipped_extension, normalize_domain

logger = logging.getLogger(__name__)

class BaseSpider(Spider):
    """
    Base spider class with common functionality for crawling.

    This spider implements the core logic for handling both single URL and domain crawls,
    with support for proxy and JavaScript rendering fallbacks.
    """

    name = 'base_spider'

    @classmethod
    def from_crawler(cls, crawler: Crawler, *args: Any, **kwargs: Any) -> 'BaseSpider':
        """
        Factory method that creates spider instance with correct settings.

        Args:
            crawler: The crawler instance
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns:
            BaseSpider: Spider instance with updated settings
        """
        # Get spider-specific settings
        spider_settings = crawler.settings.get('SPIDER_SPECIFIC_SETTINGS', {}).get(cls.name, {})

        # Update crawler settings with spider-specific settings
        if spider_settings:
            for key, value in spider_settings.items():
                crawler.settings.set(key, value, priority='spider')

        # Create spider instance
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        return spider

    def __init__(self, job_id: str = None, crawl_id: str = None, max_pages: int = 50, *args, **kwargs):
        """
        Initialize the spider with job parameters.

        Args:
            job_id: Unique identifier for this crawl job (deprecated, use crawl_id)
            crawl_id: Unique identifier for this crawl job
            max_pages: Maximum number of pages to crawl
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        super().__init__(*args, **kwargs)

        self.job_id = job_id
        self.crawl_id = crawl_id
        if not self.crawl_id:
            raise ValueError("crawl_id must be provided")

        self.custom_params = {}
        standard_params = {'job_id', 'crawl_id', 'max_pages', 'domain', 'url', 'use_sitemap', 'single_url'}
        for key, value in kwargs.items():
            if key not in standard_params:
                self.custom_params[key] = value

        # Ensure max_pages is an integer
        self.max_pages = int(max_pages) if max_pages is not None else 50
        self.pages_crawled = 0

        # Crawling strategy flags
        self.use_proxy = False  # Will be set to True if direct crawl fails
        self.use_js_rendering = False  # Will be set to True if proxy crawl fails

        # Stats tracking
        self.stats = {
            'pages_crawled': 0,
            'pages_failed': 0,
            'pages_skipped': 0,
            'skipped_urls': [],  # Track URLs that were skipped
            'proxy_used': False,
            'js_rendering_used': False,
            'direct_crawl_failures': 0,
            'proxy_crawl_failures': 0
        }

    def make_request(self, url: str, callback=None, errback=None, dont_filter=False, **kwargs) -> Request:
        """
        Create a request with appropriate meta flags based on current crawling strategy.

        Args:
            url: URL to request
            callback: Callback function for successful response
            errback: Callback function for failed request
            dont_filter: Whether to filter duplicate URLs
            **kwargs: Additional request parameters

        Returns:
            Request object with appropriate meta flags
        """
        # Check if URL has skipped extension - if so, don't make the request
        if has_skipped_extension(url):
            logger.warning(f"URL has skipped extension: {url}")

            # Track in stats
            self.stats['pages_skipped'] += 1
            if url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(url)

            # Update crawler stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_skipped')

            # Skip this URL by raising an exception that will be caught by Scrapy
            # This effectively abandons the request creation
            raise IgnoreRequest(f"URL has skipped extension: {url}")

        meta = kwargs.pop('meta', {})
        meta.update({
            'job_id': self.job_id,
            'crawl_id': self.crawl_id,  # Include crawl_id in meta
            'max_retries': 2,  # Allow 2 retries with different strategies
            'use_proxy': self.use_proxy,
            'js_render': self.use_js_rendering,
            'handle_httpstatus_list': [403, 404, 429, 500, 502, 503, 504],  # Handle these statuses in errback
        })

        return Request(
            url=url,
            callback=callback or self.parse,
            errback=errback or self.handle_error,
            dont_filter=dont_filter,
            meta=meta,
            **kwargs
        )

    def handle_error(self, failure):
        """
        Handle failed requests by trying different crawling strategies.

        The progression is:
        1. Direct crawl (no proxy, no JS)
        2. Proxy crawl (if direct fails)
        3. If homepage/index is JS-heavy, enable JS rendering for the domain

        Args:
            failure: The failure details
        """
        request = failure.request
        url = request.url

        # Check if URL has skipped extension - if so, add to skipped list and don't retry
        if has_skipped_extension(url):
            logger.warning(f"URL has skipped extension: {url}")

            # Track in stats
            self.stats['pages_skipped'] += 1
            if url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(url)

            # Update crawler stats
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_skipped')

            # Don't log this as an error
            return None

        # Continue with normal error handling for non-skipped URLs
        retries = request.meta.get('max_retries', 0)
        domain = normalize_domain(urlparse(url).netloc)

        logger.warning(f"Request failed for {url}: {failure.value}")
        # Use the crawler stats object instead of the dict
        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            self.crawler.stats.inc_value('pages_failed')
        else:
            self.stats['pages_failed'] += 1

        # Try fallback strategies if retries remain
        if retries > 0:
            new_meta = dict(request.meta)
            new_meta['max_retries'] = retries - 1

            if not self.use_proxy and not self.use_js_rendering:
                # Direct crawl failed, try with proxy
                logger.info(f"Direct crawl failed for {domain}, switching to proxy")
                self.use_proxy = True
                self.stats['proxy_used'] = True
                new_meta['use_proxy'] = True
                new_meta['js_render'] = False
                return self.make_request(url, meta=new_meta, dont_filter=True)

            elif self.use_proxy and not self.use_js_rendering:
                # Proxy crawl failed, try with JS rendering
                logger.info(f"Proxy crawl failed for {domain}, switching to JS rendering")
                self.use_js_rendering = True
                self.stats['js_rendering_used'] = True
                new_meta['js_render'] = True
                return self.make_request(url, meta=new_meta, dont_filter=True)

        # If all strategies failed, check if the page is JS-heavy and force JS rendering for the domain
        # Only do this for the homepage/index or first page
        if hasattr(self, 'force_js_render') and hasattr(request, 'url'):
            # Try to get the response from failure if available
            response = getattr(failure, 'value', None)
            if response and hasattr(response, 'text'):
                if self._is_js_heavy_response(response):
                    logger.info(f"Detected JS-heavy homepage for {domain}, enabling JS rendering for domain.")
                    self.force_js_render(domain)
                    # Retry with JS rendering
                    new_meta = dict(request.meta)
                    new_meta['js_render'] = True
                    new_meta['max_retries'] = 0  # Avoid infinite loop
                    return self.make_request(url, meta=new_meta, dont_filter=True)

        logger.error(f"All crawling strategies failed for {url}")

        # Check if too many pages have failed, and stop the crawl if so
        failed_count = 0
        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            failed_count = self.crawler.stats.get_value('pages_failed', 0)
        else:
            failed_count = self.stats.get('pages_failed', 0)

        max_failed_pages = getattr(self, 'settings', {}).get('MAX_FAILED_PAGES', 20)
        if failed_count >= max_failed_pages:
            logger.error(f"Too many failed pages ({failed_count}), stopping crawl for manual intervention.")
            raise CloseSpider(f"Too many failed pages ({failed_count}), manual intervention required.")

        return None

    def _is_js_heavy_response(self, response):
        """
        Heuristic to detect if a response is JS-heavy (minimal content, lots of scripts, SPA patterns).
        Mirrors JSRenderingMiddleware._needs_js_rendering.
        """
        import re
        body_text = response.text.lower()
        # 1. Check for empty body with head tags
        body_match = re.search(r'<body[^>]*>(.*?)</body>', body_text, re.DOTALL | re.IGNORECASE)
        head_match = re.search(r'<head[^>]*>(.*?)</head>', body_text, re.DOTALL | re.IGNORECASE)
        if body_match and head_match:
            body_content = body_match.group(1).strip()
            head_content = head_match.group(1).strip()
            if len(body_content) < 200:
                for tag in [
                    'link rel="canonical"',
                    'meta name="description"',
                    'meta property="og:',
                    'meta name="twitter:',
                    'script src=',
                ]:
                    if tag in head_content:
                        return True
        # 2. Check for common JS framework indicators
        script_count = body_text.count('<script')
        if script_count > 5:
            return True
        # 3. Check for specific JS framework signatures
        for signature in [
            'ng-app', 'ng-controller', 'ng-view', 'angular.module', 'ng-bind',
            'reactroot', 'react-root', '_reactrootcontainer', 'react.', '_reactdom',
            'vue', 'v-', '[v-', 'vuejs', 'vue.js',
            'ember', 'backbone', 'knockout',
            'spa', 'single page application',
            'require.js', 'requirejs', 'systemjs', 'webpack',
            'document.getelementbyid', 'document.getelementsby', 'window.location',
            'window.onload', 'onreadystatechange', 'domcontentloaded',
            'jquery', 'fetch(', 'axios.', 'ajax', 'xhr',
        ]:
            if signature in body_text:
                return True
        # 4. Loader patterns
        loader_patterns = [
            r'<div[^>]*id=["\"]app["\"][^>]*></div>',
            r'<div[^>]*id=["\"]root["\"][^>]*></div>',
            r'<div[^>]*class=["\"]loading["\"][^>]*>',
            r'window\.onload\s*=',
            r'document\.addeventlistener\(["\"]domcontentloaded["\"]',
        ]
        for pattern in loader_patterns:
            if re.search(pattern, body_text, re.IGNORECASE):
                return True
        # 5. Script/content ratio
        html_size = len(body_text)
        if html_size > 0:
            script_content = re.findall(r'<script[^>]*>(.*?)</script>', body_text, re.DOTALL | re.IGNORECASE)
            script_size = sum(len(s) for s in script_content)
            if script_size > 0 and script_size / html_size > 0.5:
                return True
        # 6. Few content divs, many scripts
        content_divs = len(re.findall(r'<div[^>]*class=["\"][^"\"]*content[^"\"]*["\"]', body_text, re.IGNORECASE))
        if content_divs < 2 and script_count > 3:
            return True
        return False

    def parse(self, response: Response, **kwargs) -> Iterator[Dict[str, Any]]:
        """
        Default parse method that processes responses.

        Args:
            response: The response to parse
            **kwargs: Additional keyword arguments

        Yields:
            Dict containing the parsed item with HTML content and URL
        """
        # Check for skipped extensions before processing
        if has_skipped_extension(response.url):
            logger.warning(f"URL has skipped extension, not processing: {response.url}")
            self.stats['pages_skipped'] += 1
            if response.url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(response.url)
            return

        # Check if max pages reached
        if self.pages_crawled >= self.max_pages:
            raise CloseSpider(f'Reached max pages limit: {self.max_pages}')

        self.pages_crawled += 1
        self.stats['pages_crawled'] += 1

        # Skip yielding HTML for sitemap and robots.txt
        if response.meta.get('skip_html_storage'):
            return

        logger.info(f"Processing page {self.pages_crawled}/{self.max_pages}: {response.url}")

        # Yield item with HTML content and URL
        yield {
            'url': response.url,
            'html': response.text,
            'content_type': response.headers.get('Content-Type', b'').decode('utf-8', 'ignore'),
            'status': response.status,
            'crawl_strategy': {
                'proxy_used': response.meta.get('use_proxy', False),
                'js_render_used': response.meta.get('js_render', False)
            }
        }

    def closed(self, reason: str):
        """
        Called when spider is closed.

        Args:
            reason: Why the spider was closed
        """
        # Add skipped URLs count to stats
        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            self.crawler.stats.set_value('skipped_urls_count', len(self.stats['skipped_urls']))
            self.crawler.stats.set_value('proxy_used', self.stats['proxy_used'])
            self.crawler.stats.set_value('js_rendering_used', self.stats['js_rendering_used'])

        logger.info(f"Spider closed ({reason}). Stats: {self.stats}")

        # Log skipped URLs summary
        if self.stats['skipped_urls']:
            logger.info(f"Skipped {len(self.stats['skipped_urls'])} URLs with ignored extensions")
