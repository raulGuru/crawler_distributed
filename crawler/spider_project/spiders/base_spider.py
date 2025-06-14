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
    with support for conditional proxy and JavaScript rendering strategies.

    Crawling Strategy Behavior:
    ===========================

    1. **use_proxy=True explicitly set**:
       - Forces proxy usage from the very first request
       - On failure, escalates to proxy + JS rendering

    2. **use_js_rendering=True explicitly set**:
       - Forces JS rendering from the very first request
       - On failure, escalates to proxy + JS rendering

    3. **Both use_proxy=True and use_js_rendering=True explicitly set**:
       - Uses both proxy and JS rendering from the start
       - No escalation possible; retries with same settings

    4. **Neither explicitly set (or both False)**:
       - Default fallback behavior: Direct crawl → Proxy → JS rendering
       - Traditional escalation strategy maintained for backward compatibility

    5. **Auto-detection fallback**:
       - If all strategies fail and homepage is detected as JS-heavy,
       - Automatically enables JS rendering for the domain

    Parameter Handling:
    ==================
    - Boolean parameters accept: True/False, "true"/"false", "yes"/"no", "1"/"0", "on"/"off"
    - String-to-boolean conversion is case-insensitive
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

    def __init__(self, job_id: str = None, crawl_id: str = None, max_pages: int = 50, project_id: str = None, cycle_id: int = 0, use_proxy: bool = False, use_js_rendering: bool = False, *args, **kwargs):
        """
        Initialize the spider with job parameters.

        Args:
            job_id: Unique identifier for this crawl job (deprecated, use crawl_id)
            crawl_id: Unique identifier for this crawl job
            max_pages: Maximum number of pages to crawl
            project_id: Project ID (ObjectId as string)
            cycle_id: Cycle ID (integer)
            use_proxy: Whether to use proxy (boolean or string)
            use_js_rendering: Whether to use JS rendering (boolean or string)
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        super().__init__(*args, **kwargs)

        self.job_id = job_id
        self.crawl_id = crawl_id
        if not self.crawl_id:
            raise ValueError("crawl_id must be provided")

        self.project_id = project_id
        self.cycle_id = cycle_id

        self.custom_params = {}
        standard_params = {'job_id', 'crawl_id', 'max_pages', 'domain', 'url', 'use_sitemap', 'single_url'}
        for key, value in kwargs.items():
            if key not in standard_params:
                self.custom_params[key] = value

        # Ensure max_pages is an integer
        self.max_pages = int(max_pages) if max_pages is not None else 50
        self.pages_crawled = 0

        # Convert string parameters to booleans for proper handling
        def str_to_bool(value):
            """Convert string representation to boolean."""
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', 'yes', '1', 'on')
            return bool(value)

        # Crawling strategy flags with proper boolean conversion
        self.use_proxy = str_to_bool(use_proxy)
        self.use_js_rendering = str_to_bool(use_js_rendering)

        # Store initial values to differentiate between explicit True and escalated True
        self.initial_use_proxy = self.use_proxy
        self.initial_use_js_rendering = self.use_js_rendering

        # Track if these were explicitly set to True from the start
        self.explicit_proxy_from_start = self.use_proxy is True
        self.explicit_js_rendering_from_start = self.use_js_rendering is True

        # Log initial strategy
        if self.explicit_proxy_from_start or self.explicit_js_rendering_from_start:
            strategies = []
            if self.explicit_proxy_from_start:
                strategies.append("proxy")
            if self.explicit_js_rendering_from_start:
                strategies.append("JS rendering")
            logger.info(f"Explicit crawling strategies enabled from start: {', '.join(strategies)}")

        # Stats tracking
        self.stats = {
            'pages_crawled': 0,
            'pages_failed': 0,
            'pages_skipped': 0,
            'skipped_urls': [],  # Track URLs that were skipped
            'proxy_used': self.use_proxy,
            'js_rendering_used': self.use_js_rendering,
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

        # Determine starting stage if not already provided
        if 'strategy_stage' not in meta:
            if self.use_js_rendering:
                meta['strategy_stage'] = 'js_proxy'
                meta['attempt_count'] = 1
                meta.setdefault('use_proxy', True)
                meta.setdefault('js_render', True)
            elif self.use_proxy:
                meta['strategy_stage'] = 'proxy'
                meta['attempt_count'] = 1
                meta.setdefault('use_proxy', True)
            else:
                meta['strategy_stage'] = 'direct'
                meta['attempt_count'] = 1

            # If this is the very first request and explicit values were set, force them
            if self.explicit_proxy_from_start:
                meta['use_proxy'] = True
                meta['force_proxy'] = True
                logger.info(f"Forcing proxy usage from start for {url} (explicitly requested)")

            if self.explicit_js_rendering_from_start:
                meta['js_render'] = True
                meta['force_js_render'] = True
                logger.info(f"Forcing JS rendering from start for {url} (explicitly requested)")

        # Update meta with identifiers and current flags
        meta.update({
            'job_id': self.job_id,
            'crawl_id': self.crawl_id,
            'use_proxy': meta.get('use_proxy', False),
            'js_render': meta.get('js_render', False),
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
        """Handle failed requests using staged retry logic."""
        request = failure.request
        url = request.url

        # Skip retry for ignored extensions
        if has_skipped_extension(url):
            logger.warning(f"URL has skipped extension: {url}")
            self.stats['pages_skipped'] += 1
            if url not in self.stats['skipped_urls']:
                self.stats['skipped_urls'].append(url)
            if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
                self.crawler.stats.inc_value('pages_skipped')
            return None

        stage = request.meta.get('strategy_stage', 'direct')
        attempt = request.meta.get('attempt_count', 1)
        domain = normalize_domain(urlparse(url).netloc)

        logger.warning(f"Request failed at stage {stage} attempt {attempt} for {url}: {failure.value}")

        if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats'):
            self.crawler.stats.inc_value('pages_failed')
        else:
            self.stats['pages_failed'] += 1

        new_meta = dict(request.meta)

        if stage == 'direct':
            logger.info(f"Direct crawl failed for {domain}, switching to proxy")
            new_meta.update({
                'strategy_stage': 'proxy',
                'attempt_count': 1,
                'use_proxy': True,
                'js_render': False,
            })
            self.use_proxy = True
            self.stats['proxy_used'] = True
            return self.make_request(url, meta=new_meta, dont_filter=True)

        elif stage == 'proxy' and attempt < 3:
            logger.info(f"Proxy attempt {attempt} failed for {domain}, retrying with new proxy")
            new_meta.update({
                'strategy_stage': 'proxy',
                'attempt_count': attempt + 1,
                'use_proxy': True,
                'js_render': False,
            })
            self.use_proxy = True
            self.stats['proxy_used'] = True
            return self.make_request(url, meta=new_meta, dont_filter=True)

        elif stage == 'proxy' and attempt == 3:
            logger.info(f"Proxy attempts exhausted for {domain}, switching to proxy + JS rendering")
            new_meta.update({
                'strategy_stage': 'js_proxy',
                'attempt_count': 1,
                'use_proxy': True,
                'js_render': True,
            })
            self.use_proxy = True
            self.use_js_rendering = True
            self.stats['proxy_used'] = True
            self.stats['js_rendering_used'] = True
            return self.make_request(url, meta=new_meta, dont_filter=True)

        elif stage == 'js_proxy' and attempt < 3:
            logger.info(f"JS proxy attempt {attempt} failed for {domain}, retrying with new proxy")
            new_meta.update({
                'strategy_stage': 'js_proxy',
                'attempt_count': attempt + 1,
                'use_proxy': True,
                'js_render': True,
            })
            self.use_proxy = True
            self.use_js_rendering = True
            self.stats['proxy_used'] = True
            self.stats['js_rendering_used'] = True
            return self.make_request(url, meta=new_meta, dont_filter=True)

        else:
            logger.error(f"All crawling strategies failed for {url}")

        failed_count = self.crawler.stats.get_value('pages_failed', 0) if hasattr(self, 'crawler') and hasattr(self.crawler, 'stats') else self.stats.get('pages_failed', 0)
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
            'status': response.status
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

        # logger.info(f"Spider closed ({reason}). Stats: {self.stats}")

        # Log skipped URLs summary
        if self.stats['skipped_urls']:
            logger.info(f"Skipped {len(self.stats['skipped_urls'])} URLs with ignored extensions")
