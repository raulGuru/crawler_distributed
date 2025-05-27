#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
UserAgentMiddleware for Scrapy.

This middleware handles user agent rotation for requests, allowing
different strategies for rotation (per-request, per-domain, or per-crawl).
"""

import logging
import random
from urllib.parse import urlparse
from scrapy import signals
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class UserAgentMiddleware:
    """
    Middleware for handling user agent rotation.

    This middleware allows rotating user agents based on different strategies:
    - per_request: Use a different user agent for each request
    - per_domain: Use a consistent user agent for each domain
    - per_crawl: Use a single user agent for the entire crawl
    """

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create middleware from crawler.

        Args:
            crawler: Scrapy crawler

        Returns:
            UserAgentMiddleware instance
        """
        # Get settings
        rotate_enabled = crawler.settings.getbool('ROTATE_USER_AGENT', True)
        if not rotate_enabled:
            raise NotConfigured("UserAgentMiddleware is disabled (ROTATE_USER_AGENT=False)")

        user_agents = crawler.settings.getlist('USER_AGENTS', [])
        if not user_agents:
            raise NotConfigured("USER_AGENTS setting is empty")

        rotation_policy = crawler.settings.get('USER_AGENT_ROTATION_POLICY', 'per_domain')
        default_user_agent = crawler.settings.get('DEFAULT_USER_AGENT')

        # Create middleware instance
        middleware = cls(
            user_agents=user_agents,
            rotation_policy=rotation_policy,
            default_user_agent=default_user_agent,
        )

        # Connect to signals
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def __init__(self, user_agents=None, rotation_policy='per_domain', default_user_agent=None):
        """
        Initialize the middleware.

        Args:
            user_agents (list): List of user agent strings
            rotation_policy (str): Policy for rotation ('per_request', 'per_domain', 'per_crawl')
            default_user_agent (str): Default user agent to use if none selected
        """
        self.user_agents = user_agents or []
        self.rotation_policy = rotation_policy
        self.default_user_agent = default_user_agent

        # Domain to user agent mapping (for per_domain policy)
        self.domain_ua_map = {}

        # Crawl-wide user agent (for per_crawl policy)
        self.crawl_ua = None

        # Statistics
        self.stats = {
            'user_agents_used': set(),
            'domains_with_custom_ua': set(),
            'requests_with_ua': 0,
        }

        logger.info(f"UserAgentMiddleware initialized with {len(self.user_agents)} user agents "
                   f"(policy: {rotation_policy})")

    def spider_opened(self, spider):
        """
        Called when spider is opened.

        Args:
            spider: Scrapy spider
        """
        # Set up stats
        spider.crawler.stats.set_value('user_agent/requests_with_ua', 0)

        # For per_crawl policy, select a user agent now
        if self.rotation_policy == 'per_crawl' and self.user_agents:
            self.crawl_ua = random.choice(self.user_agents)
            logger.info(f"Selected user agent for crawl: {self.crawl_ua}")
            spider.crawler.stats.set_value('user_agent/crawl_user_agent', self.crawl_ua)

    def spider_closed(self, spider):
        """
        Called when spider is closed.

        Args:
            spider: Scrapy spider
        """
        # Log user agent statistics
        logger.info(f"UserAgentMiddleware used {len(self.stats['user_agents_used'])} different user agents "
                   f"for {len(self.stats['domains_with_custom_ua'])} domains")

    def process_request(self, request, spider):
        """
        Process outgoing request to apply user agent.

        Args:
            request: Scrapy Request
            spider: Scrapy spider

        Returns:
            None: Continue processing the request
        """
        # Always set a realistic User-Agent, even if one is present
        user_agent = self._get_user_agent(request)
        if user_agent:
            request.headers['User-Agent'] = user_agent
            self.stats['requests_with_ua'] += 1
            self.stats['user_agents_used'].add(user_agent)
            spider.crawler.stats.inc_value('user_agent/requests_with_ua')
            logger.debug(f"Applied User-Agent: {user_agent} for {request.url}")

        # Add more browser-like headers
        request.headers.setdefault('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
        request.headers.setdefault('Accept-Language', 'en-US,en;q=0.9')
        request.headers.setdefault('Accept-Encoding', 'gzip, deflate, br')
        request.headers.setdefault('Connection', 'keep-alive')
        request.headers.setdefault('Upgrade-Insecure-Requests', '1')
        request.headers.setdefault('Sec-Fetch-Dest', 'document')
        request.headers.setdefault('Sec-Fetch-Mode', 'navigate')
        request.headers.setdefault('Sec-Fetch-Site', 'none')
        request.headers.setdefault('Sec-Fetch-User', '?1')
        # Enable cookies for all requests
        request.cookies = request.cookies or {}
        return None

    def _get_user_agent(self, request):
        """
        Get user agent based on rotation policy.

        Args:
            request: Scrapy Request

        Returns:
            str: Selected user agent string
        """
        if not self.user_agents:
            return self.default_user_agent

        if self.rotation_policy == 'per_request':
            # Different user agent for each request
            return random.choice(self.user_agents)

        elif self.rotation_policy == 'per_domain':
            # Consistent user agent for each domain
            domain = self._get_domain(request.url)
            if domain:
                if domain not in self.domain_ua_map:
                    # First request for this domain, select a user agent
                    self.domain_ua_map[domain] = random.choice(self.user_agents)
                    self.stats['domains_with_custom_ua'].add(domain)

                return self.domain_ua_map[domain]

            # Fallback for invalid domains
            return random.choice(self.user_agents)

        elif self.rotation_policy == 'per_crawl':
            # Same user agent for entire crawl
            if not self.crawl_ua and self.user_agents:
                self.crawl_ua = random.choice(self.user_agents)

            return self.crawl_ua

        # Unknown policy, use default
        return self.default_user_agent

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