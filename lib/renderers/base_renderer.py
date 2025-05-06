#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Base renderer interface for JavaScript rendering.

This module defines the abstract base class for JavaScript rendering
backends (like Splash and Playwright), providing a common interface
for rendering operations.
"""

import logging
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseRenderer(ABC):
    """
    Abstract base class for JavaScript rendering backends.

    This class defines the interface that all JS rendering backends
    must implement. It provides a common API for rendering pages
    regardless of the underlying technology (Splash, Playwright, etc).
    """

    def __init__(self, timeout=30, render_timeout=60, resource_timeout=30):
        """
        Initialize the renderer.

        Args:
            timeout (int): Connection timeout in seconds
            render_timeout (int): Maximum time to wait for rendering in seconds
            resource_timeout (int): Maximum time to wait for individual resources
        """
        self.timeout = timeout
        self.render_timeout = render_timeout
        self.resource_timeout = resource_timeout

        # Statistics
        self.stats = {
            'pages_rendered': 0,
            'render_errors': 0,
            'total_render_time': 0,
            'successful_renders': 0,
        }

    @abstractmethod
    def render(self, url, wait_time=2.0, wait_for_selector=None,
              wait_until=None, headers=None, cookies=None):
        """
        Render a page with JavaScript execution.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for
            headers (dict): Custom HTTP headers
            cookies (list): Cookies to set

        Returns:
            dict: Rendering result containing:
                - html: Rendered HTML content
                - status_code: HTTP status code
                - headers: Response headers
                - url: Final URL (after redirects)
                - time: Time taken to render
                - screenshot: Optional screenshot (if enabled)
                - error: Error message (if any)
        """
        pass

    @abstractmethod
    def shutdown(self):
        """
        Clean up resources and shutdown the renderer.
        """
        pass

    def get_stats(self):
        """
        Get renderer statistics.

        Returns:
            dict: Statistics about renderer usage
        """
        # Add derived stats
        stats = dict(self.stats)

        # Calculate average render time
        if stats['successful_renders'] > 0:
            stats['avg_render_time'] = stats['total_render_time'] / stats['successful_renders']
        else:
            stats['avg_render_time'] = 0

        # Calculate success rate
        if stats['pages_rendered'] > 0:
            stats['success_rate'] = (stats['successful_renders'] / stats['pages_rendered']) * 100
        else:
            stats['success_rate'] = 0

        return stats

    def _record_render_time(self, start_time, success=True):
        """
        Record rendering statistics.

        Args:
            start_time (float): Render start time
            success (bool): Whether rendering was successful

        Returns:
            float: Time taken to render
        """
        self.stats['pages_rendered'] += 1

        render_time = time.time() - start_time

        if success:
            self.stats['successful_renders'] += 1
            self.stats['total_render_time'] += render_time
        else:
            self.stats['render_errors'] += 1

        return render_time