#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Puppeteer renderer for JavaScript rendering.

This module implements the BaseRenderer interface using Pyppeteer,
a Python port of Puppeteer for JavaScript rendering. Puppeteer
provides a high-level API to control Chrome/Chromium browsers.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Union, Any

try:
    import pyppeteer
    from pyppeteer import launch
    from pyppeteer.browser import Browser
    from pyppeteer.errors import TimeoutError, NetworkError
    PUPPETEER_AVAILABLE = True
except ImportError:
    PUPPETEER_AVAILABLE = False

from lib.renderers.base_renderer import BaseRenderer

logger = logging.getLogger(__name__)


class PuppeteerRenderer(BaseRenderer):
    """
    Pyppeteer-based JavaScript renderer.

    This class implements the BaseRenderer interface using Pyppeteer,
    a Python port of Puppeteer for JavaScript rendering.
    """

    def __init__(self, headless=True, timeout=30, render_timeout=60,
                 resource_timeout=30, proxy=None, user_data_dir=None,
                 screenshot=False, window_width=1024, window_height=768,
                 browser_args=None, executable_path=None, user_agent=None):
        """
        Initialize the Puppeteer renderer.

        Args:
            headless (bool): Whether to run browser in headless mode
            timeout (int): Connection timeout in seconds
            render_timeout (int): Maximum time to wait for rendering in seconds
            resource_timeout (int): Maximum time to wait for individual resources
            proxy (str): Proxy server to use (e.g., 'http://user:pass@proxy:8080')
            user_data_dir (str): Path to user data directory for persistent sessions
            screenshot (bool): Whether to capture screenshots
            window_width (int): Width of the browser window
            window_height (int): Height of the browser window
            browser_args (list): Additional arguments to pass to browser launcher
            executable_path (str): Path to browser executable
            user_agent (str): User agent string
        """
        if not PUPPETEER_AVAILABLE:
            raise ImportError(
                "Pyppeteer is not installed. "
                "Please install it with 'pip install pyppeteer'"
            )

        super().__init__(timeout, render_timeout, resource_timeout)

        # Puppeteer configuration
        self.headless = headless
        self.proxy = proxy
        self.user_data_dir = user_data_dir
        self.screenshot = screenshot
        self.window_width = window_width
        self.window_height = window_height
        self.browser_args = browser_args or []
        self.executable_path = executable_path
        self.user_agent = user_agent

        # Puppeteer instances
        self._browser = None

        # Initialize event loop
        self._loop = asyncio.get_event_loop() if asyncio.get_event_loop().is_running() else asyncio.new_event_loop()

        # Launch browser
        self._browser = self._loop.run_until_complete(self._launch_browser())

    async def _launch_browser(self):
        """
        Launch browser with specified options.

        Returns:
            Browser: Puppeteer browser instance
        """
        launch_options = {
            'headless': self.headless,
            'args': ['--no-sandbox', '--disable-dev-shm-usage'],
            'ignoreHTTPSErrors': True,
            'defaultViewport': {
                'width': self.window_width,
                'height': self.window_height
            },
            'handleSIGINT': False,
            'handleSIGTERM': False,
            'handleSIGHUP': False,
        }

        # Add proxy if specified
        if self.proxy:
            launch_options['args'].append(f'--proxy-server={self.proxy}')

        # Add user data directory if specified
        if self.user_data_dir:
            launch_options['userDataDir'] = self.user_data_dir

        # Add additional arguments if specified
        if self.browser_args:
            launch_options['args'].extend(self.browser_args)

        # Add executable path if specified
        if self.executable_path:
            launch_options['executablePath'] = self.executable_path

        try:
            browser = await launch(**launch_options)
            logger.info("Launched Chromium browser with Pyppeteer")
            return browser
        except Exception as e:
            logger.error(f"Failed to launch browser: {str(e)}")
            raise

    async def _render_async(self, url, wait_time=2.0, wait_for_selector=None,
                           wait_until=None, headers=None, cookies=None,
                           user_agent=None):
        """
        Async method to render a URL using Pyppeteer.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for
            headers (dict): Custom HTTP headers
            cookies (list): Cookies to set
            user_agent (str): User agent string

        Returns:
            dict: Rendering result
        """
        start_time = time.time()
        page = None
        result = {
            'html': '',
            'status_code': 0,
            'headers': {},
            'url': url,
            'time': 0,
            'screenshot': None
        }

        try:
            # Create a new page
            page = await self._browser.newPage()

            # Set viewport
            await page.setViewport({
                'width': self.window_width,
                'height': self.window_height
            })

            # Set user agent if provided
            if user_agent or self.user_agent:
                await page.setUserAgent(user_agent or self.user_agent)

            # Set extra HTTP headers if provided
            if headers:
                await page.setExtraHTTPHeaders(headers)

            # Set cookies if provided
            if cookies:
                await page.setCookie(*cookies)

            # Set default navigation timeout
            page.setDefaultNavigationTimeout(self.render_timeout * 1000)  # milliseconds

            # Navigate to URL with proper wait_until option
            wait_until_option = ['load']
            if wait_until:
                if wait_until == 'networkidle':
                    wait_until_option = ['networkidle0']
                elif wait_until == 'domcontentloaded':
                    wait_until_option = ['domcontentloaded']

            response = await page.goto(
                url,
                waitUntil=wait_until_option,
                timeout=self.render_timeout * 1000  # milliseconds
            )

            # Additional wait if specified
            if wait_time:
                await page.waitFor(wait_time * 1000)  # milliseconds

            # Wait for specific element if requested
            if wait_for_selector:
                await page.waitForSelector(
                    wait_for_selector,
                    timeout=self.render_timeout * 1000  # milliseconds
                )

            # Get page content and other info
            html = await page.content()
            current_url = page.url

            response_headers = {}
            status_code = 0

            if response:
                status_code = response.status
                response_headers = response.headers

            # Take screenshot if enabled
            screenshot = None
            if self.screenshot:
                screenshot = await page.screenshot(type='png', fullPage=True)

            # Prepare result object
            result = {
                'html': html,
                'status_code': status_code,
                'headers': response_headers,
                'url': current_url,
                'time': self._record_render_time(start_time, success=True),
            }

            if screenshot:
                result['screenshot'] = screenshot

            logger.debug(f"Rendered {url} in {result['time']:.2f}s")
            return result

        except TimeoutError as e:
            error_msg = f"Timeout rendering URL {url}: {str(e)}"
            logger.error(error_msg)
            return {
                'html': '',
                'status_code': 0,
                'url': url,
                'time': self._record_render_time(start_time, success=False),
                'error': error_msg
            }
        except Exception as e:
            error_msg = f"Error rendering URL {url}: {str(e)}"
            logger.error(error_msg)
            return {
                'html': '',
                'status_code': 0,
                'url': url,
                'time': self._record_render_time(start_time, success=False),
                'error': error_msg
            }
        finally:
            # Close the page
            if page:
                await page.close()

    def render(self, url, wait_time=2.0, wait_for_selector=None,
               wait_until=None, headers=None, cookies=None,
               user_agent=None, viewport_width=None, viewport_height=None):
        """
        Render a URL using Puppeteer.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for
            headers (dict): Custom HTTP headers
            cookies (list): Cookies to set
            user_agent (str): User agent string (overrides the one set in constructor)
            viewport_width (int): Width of the viewport (overrides the one set in constructor)
            viewport_height (int): Height of the viewport (overrides the one set in constructor)

        Returns:
            dict: Rendering result
        """
        # Override instance variables if provided
        old_width = self.window_width
        old_height = self.window_height

        if viewport_width:
            self.window_width = viewport_width
        if viewport_height:
            self.window_height = viewport_height

        try:
            # Run the async rendering in the event loop
            return self._loop.run_until_complete(
                self._render_async(
                    url=url,
                    wait_time=wait_time,
                    wait_for_selector=wait_for_selector,
                    wait_until=wait_until,
                    headers=headers,
                    cookies=cookies,
                    user_agent=user_agent
                )
            )
        finally:
            # Restore original values
            self.window_width = old_width
            self.window_height = old_height

    def shutdown(self):
        """
        Clean up resources.
        """
        logger.info(f"Shutting down Puppeteer renderer. Stats: {self.get_stats()}")

        async def _shutdown_async():
            if self._browser:
                await self._browser.close()

        self._loop.run_until_complete(_shutdown_async())

        # Close the event loop if we created it
        if not asyncio._get_running_loop():
            self._loop.close()