#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Playwright renderer for JavaScript rendering.

This module implements the BaseRenderer interface using Playwright,
a modern browser automation framework that supports multiple browsers.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Union, Any

try:
    import playwright
    from playwright.sync_api import sync_playwright, ViewportSize
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from lib.renderers.base_renderer import BaseRenderer

logger = logging.getLogger(__name__)


class PlaywrightRenderer(BaseRenderer):
    """
    Playwright-based JavaScript renderer.

    This class implements the BaseRenderer interface using Playwright,
    a modern browser automation library supporting Chromium, Firefox, and WebKit.
    """

    def __init__(self, browser_type="chromium", headless=True, timeout=30, render_timeout=60,
                 resource_timeout=30, proxy=None, user_data_dir=None,
                 screenshot=False, window_width=1024, window_height=768,
                 browser_path=None, device_scale_factor=1.0, color_scheme="light",
                 locale=None, timezone_id=None, user_agent=None, ignore_https_errors=False):
        """
        Initialize the Playwright renderer.

        Args:
            browser_type (str): Browser to use ('chromium', 'firefox', or 'webkit')
            headless (bool): Whether to run browser in headless mode
            timeout (int): Connection timeout in seconds
            render_timeout (int): Maximum time to wait for rendering in seconds
            resource_timeout (int): Maximum time to wait for individual resources
            proxy (str): Proxy server to use (e.g., 'http://user:pass@proxy:8080')
            user_data_dir (str): Path to user data directory for persistent sessions
            screenshot (bool): Whether to capture screenshots
            window_width (int): Width of the browser window
            window_height (int): Height of the browser window
            browser_path (str): Path to browser executable
            device_scale_factor (float): Device scale factor for viewport
            color_scheme (str): Color scheme ('light', 'dark', or 'no-preference')
            locale (str): Browser locale
            timezone_id (str): Browser timezone (e.g., 'America/New_York')
            user_agent (str): User agent string
            ignore_https_errors (bool): Whether to ignore HTTPS errors
        """
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is not installed. "
                "Please install it with 'pip install playwright' "
                "and 'playwright install'"
            )

        super().__init__(timeout, render_timeout, resource_timeout)

        # Playwright configuration
        self.browser_type = browser_type.lower()
        self.headless = headless
        self.proxy = proxy
        self.user_data_dir = user_data_dir
        self.screenshot = screenshot
        self.window_width = window_width
        self.window_height = window_height
        self.browser_path = browser_path
        self.device_scale_factor = device_scale_factor
        self.color_scheme = color_scheme
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.ignore_https_errors = ignore_https_errors

        # Validate browser type
        if self.browser_type not in ["chromium", "firefox", "webkit"]:
            raise ValueError(f"Unsupported browser type: {self.browser_type}")

        # Initialize Playwright
        self._playwright = sync_playwright().start()
        self._browser = self._launch_browser()

    def _launch_browser(self):
        """
        Launch and configure a browser instance.

        Returns:
            Browser: Configured browser instance
        """
        logger.debug(f"Launching {self.browser_type} browser")

        # Get browser launcher based on browser_type
        if self.browser_type == "chromium":
            browser_launcher = self._playwright.chromium
        elif self.browser_type == "firefox":
            browser_launcher = self._playwright.firefox
        elif self.browser_type == "webkit":
            browser_launcher = self._playwright.webkit
        else:
            raise ValueError(f"Unsupported browser type: {self.browser_type}")

        # Prepare launch options
        launch_options = {
            "headless": self.headless,
            "timeout": self.timeout * 1000,  # Convert to milliseconds
        }

        # Add browser executable path if specified
        if self.browser_path:
            launch_options["executable_path"] = self.browser_path

        # Add user data directory if specified
        if self.user_data_dir:
            launch_options["user_data_dir"] = self.user_data_dir

        # Add proxy settings if specified
        if self.proxy:
            launch_options["proxy"] = {
                "server": self.proxy
            }

        # Add HTTPS error handling
        launch_options["ignore_https_errors"] = self.ignore_https_errors

        try:
            browser = browser_launcher.launch(**launch_options)
            logger.info(f"Launched {self.browser_type} browser")
            return browser
        except Exception as e:
            logger.error(f"Failed to launch {self.browser_type} browser: {str(e)}")
            raise

    def _create_context(self, user_agent=None, viewport_width=None, viewport_height=None):
        """
        Create a browser context with specified settings.

        Args:
            user_agent (str): User agent string
            viewport_width (int): Width of the viewport
            viewport_height (int): Height of the viewport

        Returns:
            BrowserContext: Configured browser context
        """
        # Prepare context options
        context_options = {}

        # Set viewport size
        context_options["viewport"] = {
            "width": viewport_width or self.window_width,
            "height": viewport_height or self.window_height
        }

        # Set device scale factor
        context_options["device_scale_factor"] = self.device_scale_factor

        # Set user agent if specified
        if user_agent or self.user_agent:
            context_options["user_agent"] = user_agent or self.user_agent

        # Set color scheme if specified
        if self.color_scheme:
            context_options["color_scheme"] = self.color_scheme

        # Set locale if specified
        if self.locale:
            context_options["locale"] = self.locale

        # Set timezone ID if specified
        if self.timezone_id:
            context_options["timezone_id"] = self.timezone_id

        # Create context
        return self._browser.new_context(**context_options)

    def render(self, url, wait_time=2.0, wait_for_selector=None,
               wait_until="load", headers=None, cookies=None,
               user_agent=None, viewport_width=None, viewport_height=None,
               timeout=None, js_script=None):
        """
        Render a URL using Playwright.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load in seconds
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for ('load', 'domcontentloaded', 'networkidle')
            headers (dict): Custom HTTP headers
            cookies (list): Cookies to set
            user_agent (str): User agent string (overrides the one set in constructor)
            viewport_width (int): Width of the viewport (overrides the one set in constructor)
            viewport_height (int): Height of the viewport (overrides the one set in constructor)
            timeout (int): Timeout for this specific render operation (overrides class default)
            js_script (str): JavaScript to execute after page load

        Returns:
            dict: Rendering result
        """
        start_time = time.time()

        # Use provided timeout or default
        render_timeout = (timeout or self.render_timeout) * 1000  # Convert to milliseconds

        # Create browser context with custom settings
        context = self._create_context(
            user_agent=user_agent,
            viewport_width=viewport_width,
            viewport_height=viewport_height
        )

        try:
            # Set cookies if provided
            if cookies:
                context.add_cookies(cookies)

            # Create a new page
            page = context.new_page()

            # Set extra HTTP headers if provided
            if headers:
                page.set_extra_http_headers(headers)

            # Configure navigation timeout
            page.set_default_navigation_timeout(render_timeout)
            page.set_default_timeout(render_timeout)

            # Map wait_until to Playwright waitUntil option
            if wait_until == "load":
                wait_until_option = "load"
            elif wait_until == "domcontentloaded":
                wait_until_option = "domcontentloaded"
            elif wait_until == "networkidle":
                wait_until_option = "networkidle"
            else:
                wait_until_option = "load"  # Default

            # Navigate to URL
            logger.debug(f"Loading URL: {url}")
            response = page.goto(url, wait_until=wait_until_option, timeout=render_timeout)

            # Wait additional time if specified
            if wait_time:
                page.wait_for_timeout(wait_time * 1000)  # Convert to milliseconds

            # Wait for specific element if requested
            if wait_for_selector:
                page.wait_for_selector(wait_for_selector, timeout=render_timeout)

            # Execute custom JavaScript if provided
            if js_script:
                page.evaluate(js_script)

            # Get page content and information
            html = page.content()
            current_url = page.url

            # Get status code and headers from response if available
            status_code = response.status if response else 0
            headers_dict = dict(response.headers) if response else {}

            # Take screenshot if enabled
            screenshot = None
            if self.screenshot:
                screenshot = page.screenshot(type="png")

            # Prepare result object
            result = {
                'html': html,
                'status_code': status_code,
                'headers': headers_dict,
                'url': current_url,
                'time': self._record_render_time(start_time, success=True),
            }

            if screenshot:
                result['screenshot'] = screenshot

            logger.debug(f"Rendered {url} in {result['time']:.2f}s")
            return result

        except playwright.Error as e:
            error_msg = f"Playwright error rendering URL {url}: {str(e)}"
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
            # Clean up context
            try:
                context.close()
            except Exception as e:
                logger.warning(f"Error closing browser context: {str(e)}")

    async def render_async(self, url, wait_time=2.0, wait_for_selector=None,
                          wait_until="load", headers=None, cookies=None,
                          user_agent=None, viewport_width=None, viewport_height=None,
                          timeout=None, js_script=None):
        """
        Asynchronously render a URL using Playwright.

        This method provides the same functionality as render() but in an async context.

        Args:
            Same as render() method

        Returns:
            dict: Rendering result
        """
        start_time = time.time()

        # Use provided timeout or default
        render_timeout = (timeout or self.render_timeout) * 1000  # Convert to milliseconds

        async with async_playwright() as p:
            # Get browser launcher based on browser_type
            if self.browser_type == "chromium":
                browser_launcher = p.chromium
            elif self.browser_type == "firefox":
                browser_launcher = p.firefox
            elif self.browser_type == "webkit":
                browser_launcher = p.webkit
            else:
                raise ValueError(f"Unsupported browser type: {self.browser_type}")

            # Prepare launch options
            launch_options = {
                "headless": self.headless,
                "timeout": self.timeout * 1000,  # Convert to milliseconds
            }

            # Add browser executable path if specified
            if self.browser_path:
                launch_options["executable_path"] = self.browser_path

            # Add user data directory if specified
            if self.user_data_dir:
                launch_options["user_data_dir"] = self.user_data_dir

            # Add proxy settings if specified
            if self.proxy:
                launch_options["proxy"] = {
                    "server": self.proxy
                }

            # Add HTTPS error handling
            launch_options["ignore_https_errors"] = self.ignore_https_errors

            try:
                # Launch browser
                browser = await browser_launcher.launch(**launch_options)

                # Prepare context options
                context_options = {}

                # Set viewport size
                context_options["viewport"] = {
                    "width": viewport_width or self.window_width,
                    "height": viewport_height or self.window_height
                }

                # Set device scale factor
                context_options["device_scale_factor"] = self.device_scale_factor

                # Set user agent if specified
                if user_agent or self.user_agent:
                    context_options["user_agent"] = user_agent or self.user_agent

                # Set color scheme if specified
                if self.color_scheme:
                    context_options["color_scheme"] = self.color_scheme

                # Set locale if specified
                if self.locale:
                    context_options["locale"] = self.locale

                # Set timezone ID if specified
                if self.timezone_id:
                    context_options["timezone_id"] = self.timezone_id

                # Create context
                context = await browser.new_context(**context_options)

                # Set cookies if provided
                if cookies:
                    await context.add_cookies(cookies)

                # Create a new page
                page = await context.new_page()

                # Set extra HTTP headers if provided
                if headers:
                    await page.set_extra_http_headers(headers)

                # Configure navigation timeout
                page.set_default_navigation_timeout(render_timeout)
                page.set_default_timeout(render_timeout)

                # Map wait_until to Playwright waitUntil option
                if wait_until == "load":
                    wait_until_option = "load"
                elif wait_until == "domcontentloaded":
                    wait_until_option = "domcontentloaded"
                elif wait_until == "networkidle":
                    wait_until_option = "networkidle"
                else:
                    wait_until_option = "load"  # Default

                # Navigate to URL
                logger.debug(f"Loading URL: {url}")
                response = await page.goto(url, wait_until=wait_until_option, timeout=render_timeout)

                # Wait additional time if specified
                if wait_time:
                    await page.wait_for_timeout(wait_time * 1000)  # Convert to milliseconds

                # Wait for specific element if requested
                if wait_for_selector:
                    await page.wait_for_selector(wait_for_selector, timeout=render_timeout)

                # Execute custom JavaScript if provided
                if js_script:
                    await page.evaluate(js_script)

                # Get page content and information
                html = await page.content()
                current_url = page.url

                # Get status code and headers from response if available
                status_code = response.status if response else 0
                headers_dict = dict(response.headers) if response else {}

                # Take screenshot if enabled
                screenshot = None
                if self.screenshot:
                    screenshot = await page.screenshot(type="png")

                # Prepare result object
                result = {
                    'html': html,
                    'status_code': status_code,
                    'headers': headers_dict,
                    'url': current_url,
                    'time': self._record_render_time(start_time, success=True),
                }

                if screenshot:
                    result['screenshot'] = screenshot

                # Clean up
                await context.close()
                await browser.close()

                logger.debug(f"Rendered {url} in {result['time']:.2f}s")
                return result

            except playwright.Error as e:
                error_msg = f"Playwright error rendering URL {url}: {str(e)}"
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

    def shutdown(self):
        """
        Clean up resources.
        """
        logger.info(f"Shutting down Playwright renderer. Stats: {self.get_stats()}")

        if hasattr(self, '_browser') and self._browser:
            try:
                self._browser.close()
            except Exception as e:
                logger.error(f"Error closing browser: {str(e)}")

        if hasattr(self, '_playwright') and self._playwright:
            try:
                self._playwright.stop()
            except Exception as e:
                logger.error(f"Error stopping Playwright: {str(e)}")