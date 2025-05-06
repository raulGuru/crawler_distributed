#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Middleware for handling proxy selection and rotation.
"""

import logging
from typing import Optional, Set
from scrapy import signals
from scrapy.http import Request, Response
from scrapy.spiders import Spider
from lib.utils.proxy_manager import init_proxy_manager, get_proxy_manager
from ..settings import PROXY_LIST_PATH

logger = logging.getLogger(__name__)

class ProxyMiddleware:
    """Middleware to handle proxy rotation and error handling."""

    def __init__(self, enabled: bool = True):
        """Initialize the proxy middleware."""
        self.enabled = enabled
        if enabled:
            self.domains_requiring_proxy: Set[str] = set()
            init_proxy_manager(PROXY_LIST_PATH)
        else:
            logger.info("Proxy middleware disabled")

    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware from crawler."""
        enabled = crawler.settings.getbool('PROXY_ENABLED', True)
        middleware = cls(enabled=enabled)

        if enabled:
            crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_request(self, request: Request, spider: Spider) -> Optional[Request]:
        """Process request before sending."""
        if not self.enabled:
            return None

        domain = request.url.split('/')[2]

        # Check if domain requires proxy
        if domain in self.domains_requiring_proxy:
            proxy = get_proxy_manager().get_proxy()
            if proxy:
                logger.info(f"Using proxy {proxy} for {domain}")
                request.meta['proxy'] = proxy
                request.meta['proxy_domain'] = domain
            else:
                logger.warning(f"No proxy available for {domain}")

        return None

    def process_response(self, request: Request, response: Response, spider: Spider) -> Response:
        """Process response after receiving."""
        if not self.enabled:
            return response

        domain = request.url.split('/')[2]
        proxy = request.meta.get('proxy')

        # Handle successful response
        if response.status == 200:
            if proxy:
                get_proxy_manager().mark_success(proxy)
            return response

        # Handle proxy-related errors
        if response.status in (403, 429):
            self.domains_requiring_proxy.add(domain)
            if proxy:
                get_proxy_manager().mark_banned(proxy)
                logger.warning(f"Proxy {proxy} banned for {domain}")
            else:
                # Try again with a proxy
                new_proxy = get_proxy_manager().get_proxy()
                if new_proxy:
                    logger.info(f"Retrying {domain} with proxy {new_proxy}")
                    request.meta['proxy'] = new_proxy
                    request.meta['proxy_domain'] = domain
                    request.dont_filter = True
                    return request
                else:
                    logger.error(f"No proxy available for retry on {domain}")

        # Handle other errors
        if proxy:
            get_proxy_manager().mark_failure(proxy)

        return response

    def spider_closed(self, spider: Spider):
        """Log proxy statistics when spider closes."""
        if not self.enabled:
            return

        stats = get_proxy_manager().get_stats()
        logger.info("Proxy statistics:")
        for proxy, proxy_stats in stats.items():
            logger.info(f"{proxy}: {proxy_stats}")
        logger.info(f"Domains requiring proxy: {self.domains_requiring_proxy}")
