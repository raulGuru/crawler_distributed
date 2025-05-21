"""Third Party Services Parser Worker module.

This module contains the ThirdPartyServicesWorker class which extracts third-party
service implementations from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import argparse
import re

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class ThirdPartyServicesWorker(BaseParserWorker):
    """Worker for extracting third-party service implementations from HTML files.

    This worker processes HTML files saved by the crawler, detects the presence
    of various third-party services and scripts, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the ThirdPartyServicesWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_third_party_services_tube",
            task_type="third_party_services",
            instance_id=instance_id,
        )

        # Initialize known third-party service patterns
        self.service_patterns = {
            'crazyegg': ['script.crazyegg.com'],
            'calltrackingmetrics': ['tctm.co'],
            'google_analytics': [
                'google-analytics.com/analytics.js',
                'google-analytics.com/ga.js',
                'googletagmanager.com/gtag/js',
                'window.dataLayer',
                'gtag(',
                'ga(',
                '_gaq',
                'GoogleAnalyticsObject'
            ],
            'google_tag_manager': [
                'googletagmanager.com/gtm.js',
                'googletagmanager.com/ns.html',
                'GTM-',
                'data-layer'
            ],
            'facebook_pixel': [
                'connect.facebook.net',
                'fbq(',
                'facebook-jssdk',
                'facebook-pixel'
            ],
            'hotjar': ['hotjar.com', 'hj.q', 'hjSettings'],
            'optimizely': ['optimizely.com', 'optimizelyDatafile'],
            'intercom': ['intercom.io', 'intercomSettings'],
            'zendesk': ['zendesk.com', 'zEmbed', 'zE('],
            'drift': ['drift.com', 'driftt.init'],
            'segment': ['segment.com', 'segment.io', 'analytics.load'],
            'mixpanel': ['mixpanel.com', 'mixpanel.init'],
            'adobe_analytics': ['sc.omtrdc.net', 's_code.js', 's.t()'],
            'new_relic': ['newrelic.com', 'nr-data.net', 'NREUM'],
            'vwo': ['vwo.com', 'vwo_'],
            'fullstory': ['fullstory.com', 'FS.', 'fullStory'],
            'amplitude': ['amplitude.com', 'amplitude.init'],
            'mouseflow': ['mouseflow.com', 'mouseflow', 'MF.init'],
            'clicktale': ['clicktale.net', 'ClickTale', 'WRInitTime'],
            'hubspot': ['hubspot.com', 'hs-script', 'HubSpot'],
            'lucky_orange': ['luckyorange.com', 'LuckyOrange', '__lo_'],
            'sumo': ['sumo.com', 'sumome.com', 'sumo_']
        }

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "third_party_services"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract third-party services data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted third-party services data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Initialize the services dictionary with required fields from the schema
            services = {
                'has_crazyegg': False,
                'has_calltrackingmetrics': False,
                'third_party_scripts': []
            }

            # Extract all script tags
            script_tags = soup.find_all("script")

            # Analyze script tags to detect third-party services
            self._analyze_script_tags(script_tags, services, domain)

            # Look for iframe tags that might load third-party services
            iframe_tags = soup.find_all("iframe")
            self._analyze_iframe_tags(iframe_tags, services)

            # Extract service-specific data based on our known patterns
            self._detect_known_services(script_tags, services)

            self.logger.debug(
                f"Extracted third_party_services: {services} for doc_id: {doc_id_str}"
            )

            return services

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _analyze_script_tags(self, script_tags, services, page_domain):
        """Analyze script tags to identify third-party services.

        Args:
            script_tags (list): List of script tags from BeautifulSoup.
            services (dict): Dictionary to update with findings.
            page_domain (str): Domain of the current page.
        """
        for script in script_tags:
            # Check external scripts by src attribute
            if script.has_attr('src'):
                src = script['src']
                # Skip relative paths as they're typically first-party
                if src.startswith(('http://', 'https://', '//', 'www.')):
                    # Extract domain from the src
                    if src.startswith('//'):
                        src = 'https:' + src  # Add protocol for parsing

                    script_domain = self._extract_domain(src)
                    if script_domain:
                        # Check if it's a third-party domain and not a common CDN or framework
                        common_libs = ['jquery', 'bootstrap', 'cloudflare', 'googleapis']
                        if script_domain != page_domain and not any(lib in script_domain for lib in common_libs):
                            # Add to third-party scripts if not already there
                            if script_domain not in services['third_party_scripts']:
                                services['third_party_scripts'].append(script_domain)

            # Check inline scripts for known patterns
            if script.string:
                self._analyze_inline_script(script.string, services)

    def _analyze_inline_script(self, script_content, services):
        """Analyze inline script content for third-party service patterns.

        Args:
            script_content (str): The content of the script tag.
            services (dict): Dictionary to update with findings.
        """
        # Common tracking code patterns
        tracking_patterns = [
            r'function\s*gtag\(',
            r'window\.dataLayer',
            r'fbq\(',
            r'twq\(',
            r'_linkedin_data_partner_id',
            r'twttr\.widgets',
            r'snaptr\(',
            r'pintrk\(',
            r'hs-script',
            r'Intercom\(',
            r'Drift\.'
        ]

        for pattern in tracking_patterns:
            if re.search(pattern, script_content):
                service_name = self._identify_service_from_pattern(pattern, script_content)
                if service_name and service_name not in services['third_party_scripts']:
                    services['third_party_scripts'].append(service_name)

    def _identify_service_from_pattern(self, pattern, content):
        """Identify the service name from a regex pattern and script content.

        Args:
            pattern (str): The regex pattern that matched.
            content (str): The script content.

        Returns:
            str: The identified service name or None.
        """
        if 'gtag' in pattern or 'dataLayer' in pattern:
            return 'Google Tag Manager/Analytics'
        elif 'fbq' in pattern:
            return 'Facebook Pixel'
        elif 'twq' in pattern or 'twttr' in pattern:
            return 'Twitter'
        elif 'linkedin' in pattern:
            return 'LinkedIn'
        elif 'snaptr' in pattern:
            return 'Snapchat'
        elif 'pintrk' in pattern:
            return 'Pinterest'
        elif 'hs-script' in pattern:
            return 'HubSpot'
        elif 'Intercom' in pattern:
            return 'Intercom'
        elif 'Drift' in pattern:
            return 'Drift'
        return None

    def _analyze_iframe_tags(self, iframe_tags, services):
        """Analyze iframe tags for third-party services.

        Args:
            iframe_tags (list): List of iframe tags from BeautifulSoup.
            services (dict): Dictionary to update with findings.
        """
        for iframe in iframe_tags:
            if iframe.has_attr('src'):
                src = iframe['src']
                # Check for common third-party iframes
                if 'youtube.com' in src or 'youtube-nocookie.com' in src:
                    services['has_youtube'] = True
                    if 'YouTube' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('YouTube')
                elif 'vimeo.com' in src:
                    services['has_vimeo'] = True
                    if 'Vimeo' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('Vimeo')
                elif 'googletagmanager.com' in src:
                    services['has_google_tag_manager'] = True
                    if 'Google Tag Manager' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('Google Tag Manager')
                elif 'facebook.com' in src:
                    services['has_facebook_widget'] = True
                    if 'Facebook' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('Facebook')
                elif 'twitter.com' in src:
                    services['has_twitter_widget'] = True
                    if 'Twitter' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('Twitter')

    def _detect_known_services(self, script_tags, services):
        """Detect known third-party services based on predefined patterns.

        Args:
            script_tags (list): List of script tags from BeautifulSoup.
            services (dict): Dictionary to update with findings.
        """
        for service_key, patterns in self.service_patterns.items():
            # Check for this service in script tags
            for script in script_tags:
                # Check src attribute
                if script.has_attr('src') and any(pattern in script['src'] for pattern in patterns):
                    service_name = service_key.replace('_', ' ').title()
                    services[f'has_{service_key}'] = True
                    if service_name not in services['third_party_scripts']:
                        services['third_party_scripts'].append(service_name)
                    break

                # Check inline script content
                if script.string and any(pattern in script.string for pattern in patterns):
                    service_name = service_key.replace('_', ' ').title()
                    services[f'has_{service_key}'] = True
                    if service_name not in services['third_party_scripts']:
                        services['third_party_scripts'].append(service_name)
                    break

    def _extract_domain(self, url):
        """Extract the domain from a URL.

        Args:
            url (str): The URL to extract domain from.

        Returns:
            str: The extracted domain or None if parsing fails.
        """
        try:
            # Handle protocol-relative URLs
            if url.startswith('//'):
                url = 'https:' + url

            # Simple regex to extract domain
            match = re.search(r'//([^/]+)', url)
            if match:
                domain = match.group(1)
                # Remove 'www.' prefix
                domain = re.sub(r'^www\.', '', domain)
                return domain
        except Exception as e:
            self.logger.warning(f"Error extracting domain from {url}: {e}")
        return None


def main():
    """Main entry point for the Third Party Services Parser Worker."""
    parser = argparse.ArgumentParser(description="Third Party Services Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = ThirdPartyServicesWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()