"""Mobile Data Parser Worker module.

This module contains the MobileWorker class which extracts mobile optimization
metrics from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import re
from datetime import datetime
import argparse
import time
from urllib.parse import urlparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class MobileWorker(BaseParserWorker):
    """Worker for extracting mobile optimization data from HTML files.

    This worker processes HTML files saved by the crawler, extracts various
    mobile-friendliness indicators including viewport configuration, responsive
    design elements, and AMP/mobile alternatives, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the MobileWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="htmlparser_mobile_extraction_tube",
            task_type="mobile_extraction",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "mobile_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str) -> dict:
        """Extract mobile optimization data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page being analyzed.

        Returns:
            dict: Extracted mobile data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Initialize the mobile data structure
            mobile_data = {
                "url": url,  # Set the URL from the parameter
                "has_viewport": False,
                "viewport_content": None,
                "viewport_width": None,
                "viewport_initial_scale": None,
                "viewport_user_scalable": None,
                "is_responsive": False,
                "has_mobile_friendly_meta": False,
                "has_amp_link": False,
                "amp_url": None,
                "has_alternate_mobile_url": False,
                "alternate_mobile_url": None,
                "media_queries_count": 0,
                "text_size_adjustment": None,
                "tap_target_issues": False,
                "font_size_issues": False,
                "touch_elements_spacing": "unknown",
                "plugins_used": [],
                "flash_used": False,
                "issues": [],
                "mobile_optimization_score": 100,  # Start with perfect score and subtract
                "mobile_friendly": None
            }

            # Extract viewport information
            self._extract_viewport_data(soup, mobile_data)

            # Extract mobile-specific meta tags
            self._extract_mobile_meta_tags(soup, mobile_data)

            # Extract AMP link information
            self._extract_amp_data(soup, mobile_data, url)

            # Extract alternate mobile URL information
            self._extract_alternate_mobile_url(soup, mobile_data, url)

            # Analyze style elements for media queries and text size adjustments
            self._analyze_styles(soup, mobile_data)

            # Check for mobile unfriendly elements (small font sizes, small buttons)
            self._check_mobile_unfriendly_elements(soup, mobile_data)

            # Check for plugins that don't work on mobile (Flash, Java applets)
            self._check_mobile_plugins(soup, mobile_data)

            # Calculate mobile optimization score
            self._calculate_mobile_score(mobile_data)

            # Determine overall mobile-friendliness
            self._determine_mobile_friendliness(mobile_data)

            self.logger.debug(
                f"Extracted mobile_data with {len(mobile_data['issues'])} issues for URL: {url}, doc_id: {doc_id_str}"
            )

            return mobile_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, URL: {url}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_viewport_data(self, soup, mobile_data):
        """Extract viewport meta tag data.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with viewport data.
        """
        viewport_meta = soup.find("meta", attrs={"name": "viewport"})

        if viewport_meta:
            viewport_content = viewport_meta.get("content", "")
            mobile_data["has_viewport"] = True
            mobile_data["viewport_content"] = viewport_content

            # Parse viewport content into components
            if viewport_content:
                viewport_parts = [part.strip() for part in viewport_content.split(',')]
                viewport_dict = {}

                for part in viewport_parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        viewport_dict[key.strip().lower()] = value.strip().lower()

                # Extract specific viewport properties
                mobile_data["viewport_width"] = viewport_dict.get("width")
                mobile_data["viewport_initial_scale"] = viewport_dict.get("initial-scale")
                mobile_data["viewport_user_scalable"] = viewport_dict.get("user-scalable")

                # Check for proper mobile viewport configuration
                if (mobile_data["viewport_width"] == "device-width" and
                    mobile_data["viewport_initial_scale"] == "1"):
                    mobile_data["is_responsive"] = True

                # Check for user-scalability issues (preventing zoom can be problematic for accessibility)
                if mobile_data["viewport_user_scalable"] == "no":
                    mobile_data["issues"].append("zoom_disabled")
        else:
            mobile_data["issues"].append("missing_viewport")

    def _extract_mobile_meta_tags(self, soup, mobile_data):
        """Extract mobile-specific meta tags.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with mobile meta tag data.
        """
        # Check for mobile-specific meta tags
        mobile_friendly_meta = soup.find("meta", attrs={"name": "mobile-web-app-capable"}) or \
                               soup.find("meta", attrs={"name": "apple-mobile-web-app-capable"})

        if mobile_friendly_meta:
            mobile_data["has_mobile_friendly_meta"] = True

    def _extract_amp_data(self, soup, mobile_data, url):
        """Extract AMP-related data.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with AMP data.
            url (str): The URL of the page being analyzed.
        """
        # Check for AMP link
        amp_link = soup.find("link", attrs={"rel": "amphtml"})
        if amp_link:
            mobile_data["has_amp_link"] = True
            amp_url = amp_link.get("href")
            if amp_url:
                # Convert relative URLs to absolute
                if not amp_url.startswith(('http://', 'https://')):
                    base_url = urlparse(url)
                    base_domain = f"{base_url.scheme}://{base_url.netloc}"
                    amp_url = f"{base_domain}{amp_url if amp_url.startswith('/') else f'/{amp_url}'}"

                mobile_data["amp_url"] = amp_url

                # Check if AMP is on a different domain
                amp_domain = urlparse(amp_url).netloc
                current_domain = urlparse(url).netloc

                if amp_domain != current_domain:
                    mobile_data["issues"].append("amp_on_different_domain")

    def _extract_alternate_mobile_url(self, soup, mobile_data, url):
        """Extract alternate mobile URL information.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with alternate mobile URL data.
            url (str): The URL of the page being analyzed.
        """
        # Check for alternate mobile URL (mobile subdomain or separate mobile site)
        alternate_mobile = soup.find("link", attrs={"rel": "alternate", "media": lambda x: x and "max-width" in x}) or \
                           soup.find("link", attrs={"rel": "alternate", "href": lambda x: x and ("m." in x or "mobile." in x)})

        if alternate_mobile:
            mobile_data["has_alternate_mobile_url"] = True
            alt_url = alternate_mobile.get("href")

            # Convert relative URLs to absolute
            if alt_url and not alt_url.startswith(('http://', 'https://')):
                base_url = urlparse(url)
                base_domain = f"{base_url.scheme}://{base_url.netloc}"
                alt_url = f"{base_domain}{alt_url if alt_url.startswith('/') else f'/{alt_url}'}"

            mobile_data["alternate_mobile_url"] = alt_url

            # Having a separate mobile site is not recommended for most sites today
            mobile_data["issues"].append("separate_mobile_site")

        # Check for canonical link pointing to desktop version (from mobile version)
        canonical_link = soup.find("link", attrs={"rel": "canonical"})

        # Parse the URL to check if it's a mobile URL
        parsed_url = urlparse(url)
        netloc = parsed_url.netloc.lower()
        url_contains_mobile = netloc.startswith('m.') or netloc.startswith('mobile.') or '/m/' in parsed_url.path

        if canonical_link and url_contains_mobile and canonical_link.get("href"):
            canonical_url = canonical_link.get("href")
            canonical_netloc = urlparse(canonical_url).netloc.lower()

            # Check if canonical URL points to non-mobile version
            if not canonical_netloc.startswith('m.') and not canonical_netloc.startswith('mobile.'):
                mobile_data["is_mobile_version"] = True
                mobile_data["canonical_to_desktop"] = True

    def _analyze_styles(self, soup, mobile_data):
        """Analyze style elements for media queries and text size adjustments.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with style analysis data.
        """
        # Extract inline styles
        inline_styles = soup.find_all("style")
        inline_style_content = ""

        for style in inline_styles:
            if style.string:
                inline_style_content += style.string

        # Count media queries
        media_query_count = inline_style_content.count('@media')
        mobile_data["media_queries_count"] = media_query_count

        if media_query_count > 0:
            mobile_data["is_responsive"] = True

        # Check for text size adjustment CSS properties
        text_size_patterns = ['text-size-adjust', '-webkit-text-size-adjust', '-moz-text-size-adjust', '-ms-text-size-adjust']
        for pattern in text_size_patterns:
            if pattern in inline_style_content:
                mobile_data["text_size_adjustment"] = "found"

                # Check if text resizing is disabled
                if f"{pattern}: none" in inline_style_content or f"{pattern}:none" in inline_style_content:
                    mobile_data["text_size_adjustment"] = "disabled"
                    mobile_data["issues"].append("text_size_adjustment_disabled")
                break

    def _check_mobile_unfriendly_elements(self, soup, mobile_data):
        """Check for elements that are unfriendly for mobile devices.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with unfriendly element data.
        """
        # Check for small font sizes (basic check)
        small_font_elements = soup.select('[style*="font-size: 1"], [style*="font-size:1"], [style*="font-size: 0"], [style*="font-size:0"]')
        if small_font_elements:
            mobile_data["font_size_issues"] = True
            mobile_data["issues"].append("small_font_size")

        # Check for potential tap target issues
        small_touch_elements = []

        # Check for small buttons
        small_buttons = soup.select('button[style*="width"], button[style*="height"]')
        for btn in small_buttons:
            style = btn.get('style', '')
            if 'width: 2' in style or 'height: 2' in style:
                small_touch_elements.append(btn)

        # Check for small links with small padding
        small_links = soup.select('a[style*="padding"]')
        for link in small_links:
            style = link.get('style', '')
            if 'padding: 0' in style or 'padding:0' in style:
                small_touch_elements.append(link)

        if small_touch_elements:
            mobile_data["tap_target_issues"] = True
            mobile_data["issues"].append("small_tap_targets")

        # Check for fixed-width layout
        fixed_width_indicators = soup.select('body[style*="width:"], div[style*="width: 9"], div[style*="width: 10"], div[style*="width: 11"], div[style*="width: 12"]')
        if fixed_width_indicators and not mobile_data["is_responsive"]:
            mobile_data["issues"].append("fixed_width_layout")

        # Check for horizontal overflow/scrolling (basic check)
        horizontal_overflow = soup.select('body[style*="overflow-x:visible"], body[style*="overflow-x: visible"], body[style*="overflow:visible"], body[style*="overflow: visible"]')
        if horizontal_overflow:
            mobile_data["issues"].append("horizontal_scrolling")

        # Check for interstitial indicators (can be penalty for mobile search)
        interstitial_indicators = soup.select('.modal, .popup, #overlay, .overlay, #interstitial, .interstitial')
        if interstitial_indicators:
            mobile_data["issues"].append("possible_intrusive_interstitial")

    def _check_mobile_plugins(self, soup, mobile_data):
        """Check for plugins that don't work well on mobile devices.

        Args:
            soup (BeautifulSoup): The parsed HTML.
            mobile_data (dict): Dictionary to update with plugin data.
        """
        # Check for use of plugins that don't work on mobile
        flash_elements = soup.select('object[type*="flash"], embed[type*="flash"]')
        if flash_elements:
            mobile_data["flash_used"] = True
            mobile_data["plugins_used"].append("flash")
            mobile_data["issues"].append("flash_content")

        java_applets = soup.select('applet, object[type*="java"]')
        if java_applets:
            mobile_data["plugins_used"].append("java")
            mobile_data["issues"].append("java_applets")

    def _calculate_mobile_score(self, mobile_data):
        """Calculate a mobile optimization score based on identified issues.

        Args:
            mobile_data (dict): Dictionary containing mobile data with issues.
        """
        score = 100  # Start with perfect score

        # Critical issues
        if "missing_viewport" in mobile_data["issues"]:
            score -= 40
        elif not mobile_data["is_responsive"]:
            score -= 25

        # Major issues
        if mobile_data["tap_target_issues"]:
            score -= 15
        if mobile_data["font_size_issues"]:
            score -= 15
        if mobile_data["flash_used"]:
            score -= 20
        if "horizontal_scrolling" in mobile_data["issues"]:
            score -= 20
        if "fixed_width_layout" in mobile_data["issues"]:
            score -= 15

        # Minor issues
        if "zoom_disabled" in mobile_data["issues"]:
            score -= 10
        if "text_size_adjustment_disabled" in mobile_data["issues"]:
            score -= 10
        if "separate_mobile_site" in mobile_data["issues"]:
            score -= 5
        if "possible_intrusive_interstitial" in mobile_data["issues"]:
            score -= 10

        # Ensure score stays within 0-100 range
        mobile_data["mobile_optimization_score"] = max(0, min(100, score))

    def _determine_mobile_friendliness(self, mobile_data):
        """Determine overall mobile-friendliness based on score.

        Args:
            mobile_data (dict): Dictionary containing mobile data with score.
        """
        score = mobile_data["mobile_optimization_score"]

        if score >= 80:
            mobile_data["mobile_friendly"] = "likely"
        elif score >= 60:
            mobile_data["mobile_friendly"] = "possibly"
        else:
            mobile_data["mobile_friendly"] = "unlikely"

    def process_task(self, job_data: dict) -> None:
        """Process a task with common workflow.

        Overridden to include URL in the extract_data call.

        Args:
            job_data (dict): The job data from the queue.
        """
        start_time = time.time()
        doc_id_str = job_data["document_id"]
        html_path = job_data["html_file_path"]
        url = job_data.get("url", "")  # Get URL from job data or use empty string if not provided

        self.logger.debug(f"[{self.worker_name}] Processing task for doc_id: {doc_id_str}, html_path: {html_path}, url: {url}")

        # Common validation
        self._validate_html_file(html_path, doc_id_str)

        # Read HTML content
        html_content = self._read_html_file(html_path)

        # Extract data (delegated to concrete implementation)
        extracted_data = self.extract_data(html_content, html_path, doc_id_str, url)

        # Store in MongoDB (using common method)
        self._store_in_mongodb(extracted_data, doc_id_str, self.get_data_field_name())

        processing_time = time.time() - start_time
        self.logger.info(
            f"[{self.worker_name}] Successfully processed and updated {self.task_type} for doc_id: {doc_id_str}, "
            f"url: {url} in {processing_time:.2f}s"
        )


def main():
    """Main entry point for the Mobile Data Parser Worker."""
    parser = argparse.ArgumentParser(description="Mobile Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = MobileWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()