"""Google Analytics Parser Worker module.

This module contains the GoogleAnalyticsWorker class which extracts Google Analytics
tracking codes and implementation details from saved HTML files as part of a
distributed crawl-parser system.
"""

import os
import sys
import re
import argparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class GoogleAnalyticsWorker(BaseParserWorker):
    """Worker for extracting Google Analytics tracking information from HTML files.

    This worker processes HTML files saved by the crawler, extracts various
    Google Analytics tracking codes and implementation details, and stores
    the results in MongoDB.
    """

    # Patterns for detecting Google Analytics implementations
    GA_SCRIPT_PATTERNS = [
        'google-analytics.com',
        'gtag',
        'googletagmanager.com',
        'analytics.js',
        'gtag/js',
        'ga(\'create\'',
        'ga("create"',
        '_gaq.push',
        'dataLayer.push',
        'urchinTracker'
    ]

    # Regex patterns for GA ID extraction
    UA_PATTERN = r'UA-\d+-\d+'
    GA4_PATTERN = r'G-[A-Z0-9]+'
    GTM_PATTERN = r'GTM-[A-Z0-9]+'

    # Tags for GTM container
    GTM_CONTAINER_TAGS = ['noscript', 'iframe', 'script']

    def __init__(self, instance_id: int = 0):
        """Initialize the GoogleAnalyticsWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_google_analytics_tube",
            task_type="google_analytics",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "ga_analytics"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract Google Analytics data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted Google Analytics data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Initialize the analytics data structure
            analytics_data = {
                "ga_codes": [],
                "has_ga": False,
                "ga_script_count": 0
            }

            # Process all script tags
            self._extract_from_script_tags(soup, analytics_data)

            # Look for Google Tag Manager containers
            self._extract_from_gtm_containers(soup, analytics_data)

            # Remove duplicate GA codes while preserving order
            #analytics_data["ga_codes"] = list(dict.fromkeys(analytics_data["ga_codes"]))

            self.logger.debug(
                f"Extracted GA analytics data: {analytics_data} for doc_id: {doc_id_str}"
            )

            return analytics_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_from_script_tags(self, soup, analytics_data):
        """Extract GA information from script tags.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            analytics_data (dict): Dictionary to store extracted analytics data.
        """
        # Extract from inline scripts
        script_tags = soup.find_all("script")

        for script in script_tags:
            # Check script content
            script_content = script.string if script.string else ""
            script_src = script.get("src", "")

            # Check for GA patterns in either content or src
            if (any(pattern in script_content for pattern in self.GA_SCRIPT_PATTERNS) or
                any(pattern in script_src for pattern in self.GA_SCRIPT_PATTERNS)):

                analytics_data["has_ga"] = True
                analytics_data["ga_script_count"] += 1

                # Extract tracking codes from content
                if script_content:
                    self._extract_tracking_codes(script_content, analytics_data)

                # Extract tracking codes from src attribute
                if script_src:
                    self._extract_tracking_codes(script_src, analytics_data)

    def _extract_from_gtm_containers(self, soup, analytics_data):
        """Extract Google Tag Manager containers.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            analytics_data (dict): Dictionary to store extracted analytics data.
        """
        # Look for GTM iframe implementation
        gtm_iframe = soup.find('iframe', src=lambda s: s and 'googletagmanager.com' in s)
        if gtm_iframe:
            analytics_data["has_ga"] = True
            analytics_data["ga_script_count"] += 1
            iframe_src = gtm_iframe.get("src", "")
            self._extract_tracking_codes(iframe_src, analytics_data)

        # Look for GTM noscript implementation
        gtm_noscript = soup.find('noscript', recursive=True)
        if gtm_noscript and gtm_noscript.find('iframe', src=lambda s: s and 'googletagmanager.com' in s):
            if not gtm_iframe:  # Only count it if we didn't already count the iframe
                analytics_data["has_ga"] = True
                analytics_data["ga_script_count"] += 1
            iframe = gtm_noscript.find('iframe', src=lambda s: s and 'googletagmanager.com' in s)
            if iframe:
                iframe_src = iframe.get("src", "")
                self._extract_tracking_codes(iframe_src, analytics_data)

    def _extract_tracking_codes(self, text, analytics_data):
        """Extract tracking codes using regex patterns.

        Args:
            text (str): Text to search for tracking codes.
            analytics_data (dict): Dictionary to store extracted analytics data.
        """
        # Extract Universal Analytics IDs (UA-XXXXX-Y)
        ua_matches = re.findall(self.UA_PATTERN, text)
        if ua_matches:
            analytics_data["ga_codes"].extend(ua_matches)

        # Extract GA4 measurement IDs (G-XXXXXXX)
        ga4_matches = re.findall(self.GA4_PATTERN, text)
        if ga4_matches:
            analytics_data["ga_codes"].extend(ga4_matches)

        # Extract GTM container IDs (GTM-XXXXX)
        gtm_matches = re.findall(self.GTM_PATTERN, text)
        if gtm_matches:
            analytics_data["ga_codes"].extend(gtm_matches)


def main():
    """Main entry point for the Google Analytics Parser Worker."""
    parser = argparse.ArgumentParser(description="Google Analytics Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = GoogleAnalyticsWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()