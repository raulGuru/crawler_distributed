"""AMP Data Parser Worker module.

This module contains the AmpDataWorker class which extracts data related to
Accelerated Mobile Pages (AMP) implementation from saved HTML files as part
of a distributed crawl-parser system.
"""

import os
import sys
import argparse
from urllib.parse import urlparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class AmpDataWorker(BaseParserWorker):
    """Worker for extracting AMP implementation data from HTML files.

    This worker processes HTML files saved by the crawler, extracts information
    about Accelerated Mobile Pages (AMP) implementation including AMP status,
    components, and configuration details, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the AmpDataWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_amp_tube",
            task_type="amp",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "amp_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract AMP implementation data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted AMP data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Initialize base AMP data structure
            amp_data = {
                "is_amp_page": False,
                "has_amp_html_tag": False,
                "has_amp_link": False,
                "amp_url": None,
                "canonical_url": None,
                "amp_components": [],
                "component_count": 0,
                "required_elements": {},
                "has_amp_analytics": False,
                "has_amp_carousel": False,
                "has_amp_form": False,
                "has_amp_iframe": False,
                "has_amp_social_share": False,
                "has_amp_ad": False,
                "has_amp_list": False,
                "amp_img_count": 0,
                "amp_video_count": 0,
                "amp_analytics_count": 0,
                "issues": []
            }

            # Check if this is an AMP page
            amp_data = self._check_amp_page_status(soup, amp_data)

            # Extract AMP link and URL
            amp_data = self._extract_amp_link(soup, amp_data)

            # Extract canonical URL
            amp_data = self._extract_canonical_url(soup, amp_data)

            # If this is an AMP page, extract additional AMP-specific information
            if amp_data["is_amp_page"]:
                amp_data = self._extract_amp_components(soup, amp_data)
                amp_data = self._check_required_elements(soup, amp_data)
                amp_data = self._count_amp_elements(soup, amp_data)
                amp_data = self._check_amp_issues(soup, amp_data)
            elif amp_data["has_amp_link"]:
                # Check for issues with AMP implementation on canonical pages
                amp_data = self._check_canonical_amp_issues(soup, amp_data)

            self.logger.debug(
                f"Extracted amp_data: {amp_data} for doc_id: {doc_id_str}"
            )

            return amp_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _check_amp_page_status(self, soup, amp_data):
        """Check if the page is an AMP page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        # Check for amp or ⚡ attribute on html tag
        html_tag = soup.find('html')
        if html_tag:
            # Check for either the 'amp' or '⚡' attribute on the html tag
            if html_tag.has_attr('amp') or html_tag.has_attr('⚡'):
                amp_data["is_amp_page"] = True
                amp_data["has_amp_html_tag"] = True

        return amp_data

    def _extract_amp_link(self, soup, amp_data):
        """Extract AMP link from the HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        # Find link with rel="amphtml"
        amp_link = soup.find('link', rel='amphtml')
        if amp_link and amp_link.has_attr('href'):
            amp_data["has_amp_link"] = True
            amp_data["amp_url"] = amp_link['href'].strip()

        return amp_data

    def _extract_canonical_url(self, soup, amp_data):
        """Extract canonical URL from the HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        # Find link with rel="canonical"
        canonical_link = soup.find('link', rel='canonical')
        if canonical_link and canonical_link.has_attr('href'):
            amp_data["canonical_url"] = canonical_link['href'].strip()

        return amp_data

    def _extract_amp_components(self, soup, amp_data):
        """Extract AMP component information.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        # Find script tags with custom-element or custom-template attributes
        component_scripts = soup.find_all('script', attrs={
            lambda name, value: name in ['custom-element', 'custom-template']
        })

        components = []
        for script in component_scripts:
            if script.has_attr('custom-element'):
                components.append(script['custom-element'])
            if script.has_attr('custom-template'):
                components.append(script['custom-template'])

        # Update AMP data with component information
        amp_data["amp_components"] = components
        amp_data["component_count"] = len(components)

        # Check for specific AMP components
        amp_data["has_amp_analytics"] = 'amp-analytics' in components
        amp_data["has_amp_carousel"] = 'amp-carousel' in components
        amp_data["has_amp_form"] = 'amp-form' in components
        amp_data["has_amp_iframe"] = 'amp-iframe' in components
        amp_data["has_amp_social_share"] = 'amp-social-share' in components
        amp_data["has_amp_ad"] = 'amp-ad' in components
        amp_data["has_amp_list"] = 'amp-list' in components

        return amp_data

    def _check_required_elements(self, soup, amp_data):
        """Check for required AMP elements.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        required_elements = {}

        # Check for AMP boilerplate
        boilerplate_selector = 'head > style[amp-boilerplate], head > style[amp4ads-boilerplate], head > style[amp4email-boilerplate]'
        boilerplate_style = soup.select_one(boilerplate_selector)
        required_elements['boilerplate_style'] = boilerplate_style is not None

        # Check for AMP runtime script
        runtime_selector = 'script[src*="ampproject.org/v0.js"], script[src*="ampproject.org/amp4ads-v0.js"], script[src*="ampproject.org/amp4email-v0.js"]'
        amp_runtime = soup.select_one(runtime_selector)
        required_elements['amp_runtime'] = amp_runtime is not None

        amp_data["required_elements"] = required_elements

        return amp_data

    def _count_amp_elements(self, soup, amp_data):
        """Count specific AMP elements.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        # Count specific AMP elements
        amp_data["amp_img_count"] = len(soup.find_all('amp-img'))
        amp_data["amp_video_count"] = len(soup.find_all('amp-video'))
        amp_data["amp_analytics_count"] = len(soup.find_all('amp-analytics'))

        return amp_data

    def _check_amp_issues(self, soup, amp_data):
        """Check for AMP-related issues.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        issues = []

        # Check if required elements are missing
        if not amp_data["required_elements"].get('boilerplate_style'):
            issues.append('missing_amp_boilerplate')

        if not amp_data["required_elements"].get('amp_runtime'):
            issues.append('missing_amp_runtime')

        # Check for disallowed elements in AMP pages
        disallowed_elements = ['frame', 'iframe', 'object', 'embed']
        for element in disallowed_elements:
            if soup.find(element):
                issues.append(f'disallowed_{element}_in_amp')

        # Check for non-AMP img tags (not in noscript)
        img_tags = soup.find_all('img')
        noscript_img_tags = []
        for noscript in soup.find_all('noscript'):
            noscript_img_tags.extend(noscript.find_all('img'))

        if len(img_tags) > len(noscript_img_tags):
            issues.append('non_amp_img_tag')

        # Check for missing canonical link
        if not amp_data["canonical_url"]:
            issues.append('amp_missing_canonical')

        amp_data["issues"] = issues

        return amp_data

    def _check_canonical_amp_issues(self, soup, amp_data):
        """Check for issues with AMP implementation on canonical pages.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            amp_data (dict): Current AMP data structure.

        Returns:
            dict: Updated AMP data.
        """
        issues = amp_data["issues"]

        # Check if AMP URL is on a different domain
        if amp_data["amp_url"] and amp_data["canonical_url"]:
            try:
                amp_domain = urlparse(amp_data["amp_url"]).netloc
                current_domain = urlparse(amp_data["canonical_url"]).netloc

                if amp_domain and current_domain and amp_domain != current_domain:
                    issues.append('amp_on_different_domain')
            except Exception as e:
                self.logger.warning(f"Error parsing URLs for domain comparison: {e}")

        amp_data["issues"] = issues

        return amp_data


def main():
    """Main entry point for the AMP Data Parser Worker."""
    parser = argparse.ArgumentParser(description="AMP Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = AmpDataWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()