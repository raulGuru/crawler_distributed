#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
JSRenderingMiddleware for Scrapy.

This middleware detects when pages require JavaScript rendering and
applies the appropriate renderer (Splash or Playwright). It switches
between direct fetching and JS rendering based on content analysis.
"""

import logging
import time
import re
from urllib.parse import urlparse
from collections import defaultdict
import json
from pathlib import Path

from scrapy import signals
from scrapy.exceptions import NotConfigured, IgnoreRequest
from scrapy.http import HtmlResponse
from scrapy.utils.project import data_path

logger = logging.getLogger(__name__)

# Signatures that might indicate a JavaScript-heavy page
JS_SIGNATURES = [
    # Angular
    'ng-app', 'ng-controller', 'ng-view', 'angular.module', 'ng-bind',
    # React
    'reactroot', 'react-root', '_reactrootcontainer', 'react.', '_reactdom',
    # Vue.js
    'vue', 'v-', '[v-', 'vuejs', 'vue.js',
    # Common JS frameworks
    'ember', 'backbone', 'knockout',
    # Single-page apps
    'spa', 'single page application',
    # Script loaders
    'require.js', 'requirejs', 'systemjs', 'webpack',
    # General JS indicators
    'document.getElementById', 'document.getElementsBy', 'window.location',
    'window.onload', 'onreadystatechange', 'domcontentloaded',
    'jquery', 'fetch(', 'axios.', 'ajax', 'xhr',
]

# These tags, if found in the head with empty body, suggest JS rendering is needed
EMPTY_BODY_HEAD_TAGS = [
    'link rel="canonical"',
    'meta name="description"',
    'meta property="og:',
    'meta name="twitter:',
    'script src=',
]

class JSRenderingMiddleware:
    """
    Middleware for detecting and handling JavaScript-rendered pages.

    This middleware analyzes responses to determine if they require JavaScript
    rendering and switches to the appropriate renderer when needed. It tracks
    domains that consistently require JS rendering to avoid unnecessary checks.
    """

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create middleware from crawler.

        Args:
            crawler: Scrapy crawler

        Returns:
            JSRenderingMiddleware instance
        """
        # Check if JS rendering is enabled
        if not crawler.settings.getbool('JS_RENDERING_ENABLED'):
            raise NotConfigured("JSRenderingMiddleware is disabled (JS_RENDERING_ENABLED=False)")

        # Get settings
        js_renderer = crawler.settings.get('DEFAULT_JS_RENDERER', 'splash')

        # Splash settings
        splash_url = crawler.settings.get('SPLASH_URL', 'http://localhost:8050')
        splash_wait = crawler.settings.getfloat('SPLASH_WAIT', 2.0)
        splash_timeout = crawler.settings.getint('SPLASH_TIMEOUT', 30)
        splash_js_source = crawler.settings.getbool('SPLASH_JS_SOURCE', True)

        # Playwright settings
        playwright_browser = crawler.settings.get('PLAYWRIGHT_BROWSER_TYPE', 'chromium')
        playwright_launch_options = crawler.settings.getdict('PLAYWRIGHT_LAUNCH_OPTIONS', {})
        playwright_viewport = crawler.settings.getdict('PLAYWRIGHT_DEFAULT_VIEWPORT',
                                                    {'width': 1920, 'height': 1080})
        playwright_timeout = crawler.settings.getint('PLAYWRIGHT_NAVIGATION_TIMEOUT', 30000)

        # Create middleware instance
        middleware = cls(
            js_renderer=js_renderer,
            splash_url=splash_url,
            splash_wait=splash_wait,
            splash_timeout=splash_timeout,
            splash_js_source=splash_js_source,
            playwright_browser=playwright_browser,
            playwright_launch_options=playwright_launch_options,
            playwright_viewport=playwright_viewport,
            playwright_timeout=playwright_timeout,
            crawler=crawler,
        )

        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def __init__(self, js_renderer='splash',
                splash_url='http://localhost:8050', splash_wait=2.0,
                splash_timeout=30, splash_js_source=True,
                playwright_browser='chromium', playwright_launch_options=None,
                playwright_viewport=None, playwright_timeout=30000,
                crawler=None):
        """
        Initialize the middleware.

        Args:
            js_renderer (str): Which renderer to use ('splash' or 'playwright')
            splash_url (str): URL of Splash service
            splash_wait (float): Seconds Splash should wait after page load
            splash_timeout (int): Timeout for Splash requests in seconds
            splash_js_source (bool): Whether to expose JS source in Splash responses
            playwright_browser (str): Which browser to use with Playwright
            playwright_launch_options (dict): Options for launching Playwright browser
            playwright_viewport (dict): Viewport dimensions for Playwright
            playwright_timeout (int): Navigation timeout for Playwright in milliseconds
            crawler: Scrapy crawler instance
        """
        self.js_renderer = js_renderer
        self.crawler = crawler  # Store crawler instance

        # Splash settings
        self.splash_url = splash_url
        self.splash_wait = splash_wait
        self.splash_timeout = splash_timeout
        self.splash_js_source = splash_js_source

        # Playwright settings
        self.playwright_browser = playwright_browser
        self.playwright_launch_options = playwright_launch_options or {}
        self.playwright_viewport = playwright_viewport or {'width': 1920, 'height': 1080}
        self.playwright_timeout = playwright_timeout

        # Keep track of domains that need JS rendering
        self.js_required_domains = self._load_js_domains()
        self.domain_needs_js = defaultdict(int)  # Domain -> count of detections
        self.rendered_urls = set()
        self.rendering_errors = defaultdict(int)  # Domain -> count of rendering errors

        # Performance tracking
        self.render_times = defaultdict(list)  # Domain -> list of render times

        # Statistics
        self.stats = {
            'responses_analyzed': 0,
            'js_rendering_detected': 0,
            'rendered_requests': 0,
            'domains_requiring_js': set(self.js_required_domains),
            'rendering_errors': 0,
            'avg_render_time': 0.0
        }

        logger.info(f"JSRenderingMiddleware initialized (renderer: {js_renderer})")

    def spider_opened(self, spider):
        """
        Called when spider is opened.

        Args:
            spider: Scrapy spider
        """
        # Set up stats
        spider.crawler.stats.set_value('js_rendering/responses_analyzed', 0)
        spider.crawler.stats.set_value('js_rendering/js_rendering_detected', 0)
        spider.crawler.stats.set_value('js_rendering/rendered_requests', 0)
        spider.crawler.stats.set_value('js_rendering/domains_requiring_js', len(self.js_required_domains))
        spider.crawler.stats.set_value('js_rendering/rendering_errors', 0)
        spider.crawler.stats.set_value('js_rendering/avg_render_time', 0.0)

        # Add methods to spider for direct access
        spider.js_required = self.js_required
        spider.force_js_render = self.force_js_render

    def spider_closed(self, spider):
        """
        Called when spider is closed.

        Args:
            spider: Scrapy spider
        """
        # Save domains that required JS rendering
        self._save_js_domains()

        # Log final statistics
        logger.info(f"JS Rendering Statistics:")
        logger.info(f"- Domains requiring JS: {len(self.stats['domains_requiring_js'])}")
        logger.info(f"- Total rendered requests: {self.stats['rendered_requests']}")
        logger.info(f"- Rendering errors: {self.stats['rendering_errors']}")
        if self.stats['rendered_requests'] > 0:
            logger.info(f"- Average render time: {self.stats['avg_render_time']:.2f}s")

    def process_request(self, request, spider):
        """
        Process outgoing request to determine if JS rendering is needed.

        Args:
            request: Scrapy Request
            spider: Scrapy spider

        Returns:
            None: Continue processing the request normally
            Request: New request with rendering applied
        """
        # Skip requests that already have rendering applied
        if request.meta.get('js_rendered'):
            return None

        # Skip non-HTML requests
        if 'text/html' not in request.headers.get('Accept', b'text/html').decode('utf-8', errors='ignore'):
            return None

        # Get domain
        domain = self._get_domain(request.url)

        # If domain is confirmed as requiring JS rendering, always use JS rendering (skip proxy check)
        if self.js_required(domain):
            # Set a flag in the spider for stats collection
            if hasattr(spider, 'js_rendering_domains'):
                spider.js_rendering_domains.add(domain)
            else:
                spider.js_rendering_domains = set([domain])
            return self._apply_renderer(request, domain, spider)

        # If JS rendering is forced for this request, also set the flag
        if request.meta.get('force_js_render'):
            if hasattr(spider, 'js_rendering_domains'):
                spider.js_rendering_domains.add(domain)
            else:
                spider.js_rendering_domains = set([domain])
            return self._apply_renderer(request, domain, spider)

        # Otherwise, normal logic: try proxy first, then JS rendering if needed
        if not request.meta.get('proxy') and not request.meta.get('proxy_tried'):
            logger.debug(f"Trying proxy before JS rendering for {domain}")
            request.meta['proxy_tried'] = True
            return None

        return None

    def process_response(self, request, response, spider):
        """
        Process response to detect JS-dependent content.

        Args:
            request: Scrapy Request
            response: Scrapy Response
            spider: Scrapy spider

        Returns:
            Response: Original or rendered response
        """
        # Skip already rendered responses
        if request.meta.get('js_rendered') or response.url in self.rendered_urls:
            return response

        # Only analyze HTML responses
        if not isinstance(response, HtmlResponse):
            return response

        domain = self._get_domain(response.url)

        # Check for rendering errors
        if request.meta.get('js_render_failed'):
            self.rendering_errors[domain] += 1
            self.stats['rendering_errors'] += 1
            if hasattr(spider.crawler.stats, 'inc_value'):
                spider.crawler.stats.inc_value('js_rendering/rendering_errors')

            # If too many errors, stop trying to render this domain
            if self.rendering_errors[domain] >= 3:
                logger.warning(f"Too many rendering errors for {domain}, disabling JS rendering")
                self.js_required_domains.discard(domain)
                return response

        # Skip analysis if this domain is already known to require JS rendering
        if self.js_required(domain):
            # Check if we should try proxy first
            if not request.meta.get('proxy') and not request.meta.get('proxy_tried'):
                logger.debug(f"Trying proxy before JS rendering for {domain}")
                request.meta['proxy_tried'] = True
                return response

            # Render this response
            rendered_request = self._apply_renderer(request, domain, spider)
            if rendered_request:
                logger.debug(f"Re-fetching with JS rendering: {request.url}")
                return rendered_request
            return response

        # Analyze the response to detect if JS rendering is needed
        self.stats['responses_analyzed'] += 1
        if hasattr(spider.crawler.stats, 'inc_value'):
            spider.crawler.stats.inc_value('js_rendering/responses_analyzed')

        if self._needs_js_rendering(response):
            # This page needs JS rendering
            self.stats['js_rendering_detected'] += 1
            if hasattr(spider.crawler.stats, 'inc_value'):
                spider.crawler.stats.inc_value('js_rendering/js_rendering_detected')

            # Record that this domain needs JS rendering
            self.domain_needs_js[domain] += 1

            # If we've seen this multiple times, mark the domain as requiring JS
            if self.domain_needs_js[domain] >= 2:
                logger.info(f"Domain {domain} consistently requires JS rendering. Marking for future requests.")
                self.js_required_domains.add(domain)
                self.stats['domains_requiring_js'].add(domain)
                if hasattr(spider.crawler.stats, 'inc_value'):
                    spider.crawler.stats.inc_value('js_rendering/domains_requiring_js')

            # Check if we should try proxy first
            if not request.meta.get('proxy') and not request.meta.get('proxy_tried'):
                logger.debug(f"Trying proxy before JS rendering for {domain}")
                request.meta['proxy_tried'] = True
                return response

            # Render this response
            rendered_request = self._apply_renderer(request, domain, spider)
            if rendered_request:
                logger.debug(f"Re-fetching with JS rendering: {request.url}")
                return rendered_request

        return response

    def _apply_renderer(self, request, domain, spider):
        """
        Apply the appropriate JS renderer to a request.

        Args:
            request: Scrapy Request
            domain: Domain of the request
            spider: Scrapy spider

        Returns:
            Request: New request with rendering settings
        """
        # Avoid duplicate rendering
        if request.meta.get('js_rendered'):
            return None

        url = request.url
        self.rendered_urls.add(url)

        # Start timing
        start_time = time.time()

        # Update stats
        self.stats['rendered_requests'] += 1
        if hasattr(spider.crawler.stats, 'inc_value'):
            spider.crawler.stats.inc_value('js_rendering/rendered_requests')

        # Create a copy of the request with rendering applied
        new_request = request.copy()
        new_request.meta['js_rendered'] = True
        new_request.meta['js_renderer'] = self.js_renderer
        new_request.meta['render_start_time'] = start_time
        new_request.dont_filter = True  # Avoid duplicate filtering

        if self.js_renderer == 'splash':
            # Apply Splash rendering
            splash_args = {
                'wait': self.splash_wait,
                'timeout': self.splash_timeout,
                'js_source': self.splash_js_source,
                'images': 0,  # Don't load images to save bandwidth
                'render_all': 0,  # Don't render offscreen elements
                'http_method': request.method,
            }

            # Copy original headers and add UA if needed
            headers = dict(request.headers)
            if b'User-Agent' not in headers and spider.settings.get('USER_AGENT'):
                headers[b'User-Agent'] = spider.settings.get('USER_AGENT').encode('utf-8')

            new_request.meta['splash'] = {
                'args': splash_args,
                'endpoint': 'render.html',
                'headers': headers,
                'dont_process_response': True,  # We'll handle the response ourselves
            }

            # Add error callback
            new_request.errback = self._handle_render_error

            logger.debug(f"Applying Splash renderer to {url}")

        elif self.js_renderer == 'playwright':
            # Apply Playwright rendering
            new_request.meta['playwright'] = True
            new_request.meta['playwright_include_page'] = True
            new_request.meta['playwright_page_methods'] = [
                {
                    "method": "wait_for_load_state",
                    "args": ["networkidle"],
                },
                {
                    "method": "wait_for_selector",
                    "args": ["body"],
                    "kwargs": {"timeout": 10000},
                }
            ]
            new_request.meta['playwright_browser_type'] = self.playwright_browser
            new_request.meta['playwright_launch_options'] = self.playwright_launch_options
            new_request.meta['playwright_context_args'] = {
                'viewport': self.playwright_viewport,
            }
            new_request.meta['playwright_page_goto_kwargs'] = {
                'timeout': self.playwright_timeout,
                'wait_until': 'networkidle',
            }

            # Add error callback
            new_request.errback = self._handle_render_error

            logger.debug(f"Applying Playwright renderer to {url}")

        return new_request

    def _handle_render_error(self, failure):
        """
        Handle errors during rendering.

        Args:
            failure: Twisted failure
        """
        request = failure.request
        domain = self._get_domain(request.url)

        # Check if this is a max pages limit error
        if (
            (isinstance(failure.value, str) and "max pages limit" in failure.value.lower()) or
            (hasattr(failure.value, 'args') and any('max pages limit' in str(arg).lower() for arg in getattr(failure.value, 'args', [])))
        ):
            logger.info(f"JS rendering stopped for {request.url}: max_pages limit reached. No further rendering attempts will be made.")
            return None

        logger.error(f"Rendering error for {request.url}: {failure.value}")

        # Mark this request as failed
        request.meta['js_render_failed'] = True
        self.rendering_errors[domain] += 1

        # If using Splash and it's down, try switching to Playwright
        if self.js_renderer == 'splash' and isinstance(failure.value, ConnectionRefusedError):
            logger.warning("Splash appears to be down, switching to Playwright")
            self.js_renderer = 'playwright'
            return self._apply_renderer(request, domain, request.spider)

        return request

    def _needs_js_rendering(self, response):
        """
        Analyze a response to determine if it needs JavaScript rendering.

        Args:
            response: Scrapy Response

        Returns:
            bool: True if JS rendering is likely needed
        """
        # Quick check for obvious indicators
        body_text = response.text.lower()

        # 1. Check for empty body with head tags
        body_match = re.search(r'<body[^>]*>(.*?)</body>', body_text, re.DOTALL | re.IGNORECASE)
        head_match = re.search(r'<head[^>]*>(.*?)</head>', body_text, re.DOTALL | re.IGNORECASE)

        if body_match and head_match:
            body_content = body_match.group(1).strip()
            head_content = head_match.group(1).strip()

            # If body is nearly empty but head has typical meta tags, might be JS-rendered
            if len(body_content) < 200:  # Very little content in body
                for tag in EMPTY_BODY_HEAD_TAGS:
                    if tag in head_content:
                        logger.debug(f"Detected JS rendering need: Empty body with {tag} in head")
                        return True

        # 2. Check for common JS framework indicators
        script_count = body_text.count('<script')
        if script_count > 5:  # Many script tags usually indicate JS-heavy page
            logger.debug(f"Detected JS rendering need: Many script tags ({script_count})")
            return True

        # 3. Check for specific JS framework signatures
        for signature in JS_SIGNATURES:
            if signature in body_text:
                logger.debug(f"Detected JS rendering need: Found signature '{signature}'")
                return True

        # 4. Check for content loader patterns
        loader_patterns = [
            r'<div[^>]*id=["\']app["\'][^>]*></div>',  # Empty app div
            r'<div[^>]*id=["\']root["\'][^>]*></div>',  # Empty root div
            r'<div[^>]*class=["\']loading["\'][^>]*>',  # Loading indicator
            r'window\.onload\s*=',  # onload handler
            r'document\.addEventListener\(["\']DOMContentLoaded["\']',  # DOMContentLoaded
        ]

        for pattern in loader_patterns:
            if re.search(pattern, body_text, re.IGNORECASE):
                logger.debug(f"Detected JS rendering need: Found loader pattern '{pattern}'")
                return True

        # 5. Simple content/script ratio heuristic
        html_size = len(body_text)
        if html_size > 0:
            # Count script content
            script_content = re.findall(r'<script[^>]*>(.*?)</script>', body_text, re.DOTALL | re.IGNORECASE)
            script_size = sum(len(s) for s in script_content)

            # If more than 50% of the page is script content, might need rendering
            if script_size > 0 and script_size / html_size > 0.5:
                logger.debug(f"Detected JS rendering need: High script/content ratio ({script_size/html_size:.2f})")
                return True

        # 6. Check for lack of content divs with lots of script tags
        content_divs = len(re.findall(r'<div[^>]*class=["\'][^"\']*content[^"\']*["\']', body_text, re.IGNORECASE))
        if content_divs < 2 and script_count > 3:
            logger.debug(f"Detected JS rendering need: Few content divs ({content_divs}) with many scripts ({script_count})")
            return True

        return False

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

    def js_required(self, domain):
        """
        Check if a domain requires JavaScript rendering.

        This method is also exposed to the spider.

        Args:
            domain (str): Domain to check

        Returns:
            bool: True if the domain requires JS rendering
        """
        return domain in self.js_required_domains

    def force_js_render(self, domain):
        """
        Force a domain to use JavaScript rendering.

        This method is exposed to the spider.

        Args:
            domain (str): Domain to force JS rendering for
        """
        if domain not in self.js_required_domains:
            logger.info(f"Forcing JS rendering for domain: {domain}")
            self.js_required_domains.add(domain)
            self.stats['domains_requiring_js'].add(domain)

    def _load_js_domains(self):
        """Load persisted JS-requiring domains from disk."""
        try:
            js_domains_file = self.crawler.settings.get('JS_DOMAINS_FILE')
            if js_domains_file and Path(js_domains_file).exists():
                with open(js_domains_file) as f:
                    return set(json.load(f))
        except Exception as e:
            logger.warning(f"Error loading JS domains: {e}")
        return set()

    def _save_js_domains(self):
        """Save JS-requiring domains to disk."""
        try:
            js_domains_file = self.crawler.settings.get('JS_DOMAINS_FILE')
            if js_domains_file:
                Path(js_domains_file).parent.mkdir(parents=True, exist_ok=True)
                with open(js_domains_file, 'w') as f:
                    json.dump(list(self.js_required_domains), f)
        except Exception as e:
            logger.warning(f"Error saving JS domains: {e}")