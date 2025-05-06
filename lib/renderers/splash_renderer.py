#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Splash renderer for JavaScript rendering.

This module implements the BaseRenderer interface using Splash
for JavaScript rendering. Splash is a lightweight web browser with an HTTP API.
"""

import json
import logging
import time
import urllib.parse

import requests
from requests.exceptions import RequestException

from lib.renderers.base_renderer import BaseRenderer

logger = logging.getLogger(__name__)


class SplashRenderer(BaseRenderer):
    """
    Splash-based JavaScript renderer.

    This class implements the BaseRenderer interface using Splash
    for JavaScript rendering. It connects to a Splash instance via HTTP
    and uses its Lua script capability for flexible rendering options.
    """

    def __init__(self, splash_url='http://localhost:8050',
                 timeout=30, render_timeout=60, resource_timeout=30,
                 lua_source=None, screenshot=False, screenshot_width=1024,
                 screenshot_height=768, headers=None):
        """
        Initialize the Splash renderer.

        Args:
            splash_url (str): URL of the Splash instance
            timeout (int): Connection timeout in seconds
            render_timeout (int): Maximum time to wait for rendering in seconds
            resource_timeout (int): Maximum time to wait for individual resources
            lua_source (str): Custom Lua script for rendering
            screenshot (bool): Whether to capture screenshots
            screenshot_width (int): Width of the screenshot
            screenshot_height (int): Height of the screenshot
            headers (dict): Default headers to use for all requests
        """
        super().__init__(timeout, render_timeout, resource_timeout)

        # Splash configuration
        self.splash_url = splash_url
        self.screenshot = screenshot
        self.screenshot_width = screenshot_width
        self.screenshot_height = screenshot_height
        self.default_headers = headers or {}

        # Get or create the default Lua script for rendering
        self.lua_source = lua_source or self._get_default_lua_script()

        # Check Splash connectivity
        self._check_splash_connectivity()

    def _check_splash_connectivity(self):
        """
        Check if Splash is accessible.

        Raises:
            RuntimeError: If Splash is not accessible
        """
        try:
            response = requests.get(
                self.splash_url + '/_ping',
                timeout=self.timeout
            )
            if response.status_code != 200 or response.text != 'ok':
                raise RuntimeError(f"Splash returned unexpected response: {response.text}")
            logger.info(f"Successfully connected to Splash at {self.splash_url}")
        except RequestException as e:
            logger.error(f"Failed to connect to Splash at {self.splash_url}: {e}")
            raise RuntimeError(f"Could not connect to Splash: {e}")

    def _get_default_lua_script(self):
        """
        Get the default Lua script for rendering.

        Returns:
            str: Lua script for Splash
        """
        return '''
        function main(splash, args)
            -- Set timeout for the page load
            splash:set_user_agent(args.user_agent or "Scrapy/Splash Renderer (+https://scrapy.org)")

            -- Apply custom headers if provided
            if args.headers then
                splash:set_custom_headers(args.headers)
            end

            -- Set cookies if provided
            if args.cookies then
                splash:init_cookies(args.cookies)
            end

            -- Configure resource timeouts
            splash.resource_timeout = args.resource_timeout

            -- Enable request history tracking
            splash.har_reset()

            -- Set viewport size
            splash:set_viewport_size(args.width or 1024, args.height or 768)

            -- Load the page
            local ok, reason = splash:go(args.url)
            if not ok then
                return {
                    error = reason,
                    status_code = 0,
                    html = "",
                    url = args.url
                }
            end

            -- Wait for page to render completely
            if args.wait_until == "networkidle" then
                splash:wait_for_resume(args.render_timeout)
            else
                splash:wait(args.wait_time or 2.0)
            end

            -- Wait for specific element if requested
            if args.wait_for_selector then
                splash:wait_for_element(args.wait_for_selector, args.render_timeout)
            end

            -- Get the page HTML
            local html = splash:html()

            -- Get HAR data
            local har = splash.har()

            -- Get response information
            local last_response = splash:get_last_response()
            local headers = last_response.headers
            local status_code = last_response.status

            -- Take screenshot if requested
            local screenshot = nil
            if args.screenshot then
                screenshot = splash:png()
            end

            return {
                html = html,
                har = har,
                status_code = status_code,
                headers = headers,
                url = splash:url(),
                screenshot = screenshot
            }
        end
        '''

    def render(self, url, wait_time=2.0, wait_for_selector=None,
               wait_until=None, headers=None, cookies=None,
               user_agent=None, viewport_width=None, viewport_height=None):
        """
        Render a URL using Splash.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for
            headers (dict): Custom HTTP headers
            cookies (list): Cookies to set
            user_agent (str): User agent string
            viewport_width (int): Width of the viewport
            viewport_height (int): Height of the viewport

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
        start_time = time.time()

        # Prepare request parameters
        all_headers = dict(self.default_headers)
        if headers:
            all_headers.update(headers)

        splash_args = {
            'url': url,
            'wait_time': wait_time,
            'wait_for_selector': wait_for_selector,
            'wait_until': wait_until,
            'headers': all_headers,
            'cookies': cookies,
            'render_timeout': self.render_timeout,
            'resource_timeout': self.resource_timeout,
            'user_agent': user_agent,
            'width': viewport_width or self.screenshot_width,
            'height': viewport_height or self.screenshot_height,
            'screenshot': self.screenshot,
            'lua_source': self.lua_source,
        }

        # Filter out None values
        splash_args = {k: v for k, v in splash_args.items() if v is not None}

        # Make the request to Splash
        try:
            response = requests.post(
                self.splash_url + '/execute',
                json=splash_args,
                timeout=self.timeout
            )

            # Handle HTTP errors
            if response.status_code != 200:
                error_msg = f"Splash returned HTTP {response.status_code}: {response.text}"
                logger.error(error_msg)
                return {
                    'html': '',
                    'status_code': response.status_code,
                    'url': url,
                    'time': self._record_render_time(start_time, success=False),
                    'error': error_msg
                }

            # Parse the result
            result = response.json()

            # Check for errors in the result
            if 'error' in result and result['error']:
                logger.error(f"Splash rendering error: {result['error']}")
                return {
                    'html': result.get('html', ''),
                    'status_code': result.get('status_code', 0),
                    'url': result.get('url', url),
                    'time': self._record_render_time(start_time, success=False),
                    'error': result['error']
                }

            # Prepare the successful result
            render_result = {
                'html': result.get('html', ''),
                'status_code': result.get('status_code', 200),
                'headers': result.get('headers', {}),
                'url': result.get('url', url),
                'time': self._record_render_time(start_time, success=True),
                'har': result.get('har', {})
            }

            # Add screenshot if available
            if self.screenshot and 'screenshot' in result:
                render_result['screenshot'] = result['screenshot']

            logger.debug(f"Rendered {url} in {render_result['time']:.2f}s")
            return render_result

        except RequestException as e:
            error_msg = f"Error connecting to Splash: {str(e)}"
            logger.error(error_msg)
            return {
                'html': '',
                'status_code': 0,
                'url': url,
                'time': self._record_render_time(start_time, success=False),
                'error': error_msg
            }
        except Exception as e:
            error_msg = f"Unexpected error rendering URL {url}: {str(e)}"
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

        For Splash, this is a no-op as the HTTP client doesn't need cleanup.
        """
        logger.info(f"Shutting down Splash renderer. Stats: {self.get_stats()}")
        # Nothing to clean up for Splash