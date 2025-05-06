#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Selenium renderer for JavaScript rendering.

This module implements the BaseRenderer interface using Selenium,
a popular browser automation framework. It supports multiple browsers
including Chrome, Firefox, and Edge.
"""

import logging
import time
from typing import Dict, List, Optional, Union, Any

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.edge.options import Options as EdgeOptions
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from lib.renderers.base_renderer import BaseRenderer

logger = logging.getLogger(__name__)


class SeleniumRenderer(BaseRenderer):
    """
    Selenium-based JavaScript renderer.

    This class implements the BaseRenderer interface using Selenium WebDriver,
    supporting multiple browsers including Chrome, Firefox, and Edge.
    """

    def __init__(self, browser_type="chrome", headless=True, timeout=30, render_timeout=60,
                 resource_timeout=30, proxy=None, user_data_dir=None,
                 screenshot=False, window_width=1024, window_height=768,
                 browser_binary=None, executable_path=None, user_agent=None,
                 browser_arguments=None):
        """
        Initialize the Selenium renderer.

        Args:
            browser_type (str): Browser to use ('chrome', 'firefox', or 'edge')
            headless (bool): Whether to run browser in headless mode
            timeout (int): Connection timeout in seconds
            render_timeout (int): Maximum time to wait for rendering in seconds
            resource_timeout (int): Maximum time to wait for individual resources
            proxy (str): Proxy server to use (e.g., 'http://user:pass@proxy:8080')
            user_data_dir (str): Path to user data directory for persistent sessions
            screenshot (bool): Whether to capture screenshots
            window_width (int): Width of the browser window
            window_height (int): Height of the browser window
            browser_binary (str): Path to browser binary
            executable_path (str): Path to WebDriver executable
            user_agent (str): User agent string
            browser_arguments (list): Additional arguments to pass to browser
        """
        if not SELENIUM_AVAILABLE:
            raise ImportError(
                "Selenium is not installed. "
                "Please install it with 'pip install selenium'"
            )

        super().__init__(timeout, render_timeout, resource_timeout)

        # Selenium configuration
        self.browser_type = browser_type.lower()
        self.headless = headless
        self.proxy = proxy
        self.user_data_dir = user_data_dir
        self.screenshot = screenshot
        self.window_width = window_width
        self.window_height = window_height
        self.browser_binary = browser_binary
        self.executable_path = executable_path
        self.user_agent = user_agent
        self.browser_arguments = browser_arguments or []

        # Validate browser type
        if self.browser_type not in ["chrome", "firefox", "edge"]:
            raise ValueError(f"Unsupported browser type: {self.browser_type}")

        # Initialize WebDriver
        self._driver = self._create_driver()

    def _create_driver(self):
        """
        Create and configure a WebDriver instance based on browser_type.

        Returns:
            WebDriver: Configured WebDriver instance
        """
        if self.browser_type == "chrome":
            return self._create_chrome_driver()
        elif self.browser_type == "firefox":
            return self._create_firefox_driver()
        elif self.browser_type == "edge":
            return self._create_edge_driver()
        else:
            raise ValueError(f"Unsupported browser type: {self.browser_type}")

    def _create_chrome_driver(self):
        """
        Create and configure a Chrome WebDriver instance.

        Returns:
            WebDriver: Configured Chrome WebDriver instance
        """
        options = ChromeOptions()

        # Set headless mode
        if self.headless:
            options.add_argument("--headless=new")

        # Set window size
        options.add_argument(f"--window-size={self.window_width},{self.window_height}")

        # Set common arguments for stability
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        # Add user agent if specified
        if self.user_agent:
            options.add_argument(f"--user-agent={self.user_agent}")

        # Add proxy if specified
        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")

        # Add user data directory if specified
        if self.user_data_dir:
            options.add_argument(f"--user-data-dir={self.user_data_dir}")

        # Add browser binary location if specified
        if self.browser_binary:
            options.binary_location = self.browser_binary

        # Add additional arguments if specified
        for arg in self.browser_arguments:
            options.add_argument(arg)

        # Create driver with specified options
        driver_kwargs = {"options": options}
        if self.executable_path:
            driver_kwargs["executable_path"] = self.executable_path

        try:
            driver = webdriver.Chrome(**driver_kwargs)
            logger.info("Created Chrome WebDriver instance")
            return driver
        except Exception as e:
            logger.error(f"Failed to create Chrome WebDriver: {str(e)}")
            raise

    def _create_firefox_driver(self):
        """
        Create and configure a Firefox WebDriver instance.

        Returns:
            WebDriver: Configured Firefox WebDriver instance
        """
        options = FirefoxOptions()

        # Set headless mode
        if self.headless:
            options.add_argument("--headless")

        # Add user agent if specified
        if self.user_agent:
            options.set_preference("general.useragent.override", self.user_agent)

        # Add proxy if specified
        if self.proxy:
            proxy_parts = self.proxy.split("://")
            if len(proxy_parts) == 2:
                protocol, address = proxy_parts
                options.set_preference("network.proxy.type", 1)
                options.set_preference(f"network.proxy.{protocol}", address.split(":")[0])
                if ":" in address:
                    options.set_preference(f"network.proxy.{protocol}_port", int(address.split(":")[1]))

        # Add user data directory if specified
        if self.user_data_dir:
            options.set_preference("profile", self.user_data_dir)

        # Add additional arguments if specified
        for arg in self.browser_arguments:
            options.add_argument(arg)

        # Create driver with specified options
        driver_kwargs = {"options": options}
        if self.browser_binary:
            driver_kwargs["firefox_binary"] = self.browser_binary
        if self.executable_path:
            driver_kwargs["executable_path"] = self.executable_path

        try:
            driver = webdriver.Firefox(**driver_kwargs)
            driver.set_window_size(self.window_width, self.window_height)
            logger.info("Created Firefox WebDriver instance")
            return driver
        except Exception as e:
            logger.error(f"Failed to create Firefox WebDriver: {str(e)}")
            raise

    def _create_edge_driver(self):
        """
        Create and configure an Edge WebDriver instance.

        Returns:
            WebDriver: Configured Edge WebDriver instance
        """
        options = EdgeOptions()

        # Set headless mode
        if self.headless:
            options.add_argument("--headless")

        # Set window size
        options.add_argument(f"--window-size={self.window_width},{self.window_height}")

        # Set common arguments for stability
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Add user agent if specified
        if self.user_agent:
            options.add_argument(f"--user-agent={self.user_agent}")

        # Add proxy if specified
        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")

        # Add user data directory if specified
        if self.user_data_dir:
            options.add_argument(f"--user-data-dir={self.user_data_dir}")

        # Add additional arguments if specified
        for arg in self.browser_arguments:
            options.add_argument(arg)

        # Create driver with specified options
        driver_kwargs = {"options": options}
        if self.executable_path:
            driver_kwargs["executable_path"] = self.executable_path

        try:
            driver = webdriver.Edge(**driver_kwargs)
            logger.info("Created Edge WebDriver instance")
            return driver
        except Exception as e:
            logger.error(f"Failed to create Edge WebDriver: {str(e)}")
            raise

    def render(self, url, wait_time=2.0, wait_for_selector=None,
               wait_until="load", headers=None, cookies=None,
               user_agent=None, viewport_width=None, viewport_height=None):
        """
        Render a URL using Selenium WebDriver.

        Args:
            url (str): URL to render
            wait_time (float): Time to wait after page load in seconds
            wait_for_selector (str): CSS selector to wait for
            wait_until (str): Page load state to wait for ('load', 'domcontentloaded', 'networkidle')
            headers (dict): Custom HTTP headers (limited support)
            cookies (list): Cookies to set
            user_agent (str): User agent string (overrides the one set in constructor)
            viewport_width (int): Width of the viewport (overrides the one set in constructor)
            viewport_height (int): Height of the viewport (overrides the one set in constructor)

        Returns:
            dict: Rendering result
        """
        start_time = time.time()
        result = {
            'html': '',
            'status_code': 0,  # Selenium doesn't provide status codes directly
            'headers': {},     # Selenium doesn't provide response headers directly
            'url': url,
            'time': 0,
            'screenshot': None
        }

        # Apply temporary viewport changes if specified
        if viewport_width or viewport_height:
            self._driver.set_window_size(
                viewport_width or self.window_width,
                viewport_height or self.window_height
            )

        # Apply temporary user agent if specified and browser is Chrome
        if user_agent and self.browser_type == "chrome":
            old_user_agent = self._driver.execute_script("return navigator.userAgent")
            self._driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})

        try:
            # Set cookies if provided
            if cookies:
                self._driver.get("about:blank")  # Navigate to blank page first
                for cookie in cookies:
                    self._driver.add_cookie(cookie)

            # Load the URL
            logger.debug(f"Loading URL: {url}")
            self._driver.get(url)

            # Wait for page to load according to wait_until parameter
            if wait_until == "domcontentloaded":
                # Wait for DOMContentLoaded
                WebDriverWait(self._driver, self.render_timeout).until(
                    lambda d: d.execute_script("return document.readyState") != "loading"
                )
            elif wait_until == "networkidle":
                # Simple approximation of networkidle
                # Wait for document.readyState to be complete
                WebDriverWait(self._driver, self.render_timeout).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                # Then additional wait time for any async requests
                time.sleep(1.0)
            else:  # Default: "load"
                # Wait for document.readyState to be complete
                WebDriverWait(self._driver, self.render_timeout).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )

            # Additional wait if specified
            if wait_time:
                time.sleep(wait_time)

            # Wait for specific element if requested
            if wait_for_selector:
                WebDriverWait(self._driver, self.render_timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
                )

            # Get page content and URL
            html = self._driver.page_source
            current_url = self._driver.current_url

            # Take screenshot if enabled
            screenshot = None
            if self.screenshot:
                screenshot = self._driver.get_screenshot_as_png()

            # Prepare result object
            result = {
                'html': html,
                'status_code': 200,  # Assume 200 if page loaded successfully
                'headers': {},       # Not available in Selenium
                'url': current_url,
                'time': self._record_render_time(start_time, success=True),
            }

            if screenshot:
                result['screenshot'] = screenshot

            logger.debug(f"Rendered {url} in {result['time']:.2f}s")
            return result

        except TimeoutException as e:
            error_msg = f"Timeout rendering URL {url}: {str(e)}"
            logger.error(error_msg)
            return {
                'html': '',
                'status_code': 0,
                'url': url,
                'time': self._record_render_time(start_time, success=False),
                'error': error_msg
            }
        except WebDriverException as e:
            error_msg = f"WebDriver error rendering URL {url}: {str(e)}"
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
            # Restore user agent if it was temporarily changed
            if user_agent and self.browser_type == "chrome" and 'old_user_agent' in locals():
                self._driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": old_user_agent})

            # Restore original window size if it was temporarily changed
            if viewport_width or viewport_height:
                self._driver.set_window_size(self.window_width, self.window_height)

    def shutdown(self):
        """
        Clean up resources.
        """
        logger.info(f"Shutting down Selenium renderer. Stats: {self.get_stats()}")

        if self._driver:
            try:
                self._driver.quit()
            except Exception as e:
                logger.error(f"Error shutting down WebDriver: {str(e)}")
                # Try to force close if regular quit fails
                try:
                    self._driver.close()
                except:
                    pass