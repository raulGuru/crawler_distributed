"""Third Party Services Parser Worker module.

This module contains the ThirdPartyServicesWorker class which extracts third-party
service implementations and CMS/theme information from saved HTML files as part
of a distributed crawl-parser system.
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
    """Worker for extracting third-party service implementations and theme detection from HTML files.

    This worker processes HTML files saved by the crawler, detects the presence
    of various third-party services, CMS platforms, themes, and frameworks,
    and stores the results in MongoDB.
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
            'callrail': ['callrail.com', 'callrail', 'CallRail', 'cr.call'],
            'bing_webmaster': ['msvalidate', 'bing.com/webmaster'],
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

        # CMS detection patterns
        self.cms_patterns = {
            'wordpress': {
                'paths': ['/wp-content/', '/wp-includes/', '/wp-admin/'],
                'meta': ['WordPress'],
                'comments': ['WordPress']
            },
            'drupal': {
                'paths': ['/sites/default/', '/modules/', '/themes/'],
                'meta': ['Drupal'],
                'comments': ['Drupal']
            },
            'joomla': {
                'paths': ['/templates/', '/components/', '/modules/'],
                'meta': ['Joomla'],
                'comments': ['Joomla']
            },
            'shopify': {
                'paths': ['/assets/', 'cdn.shopify.com'],
                'meta': ['Shopify'],
                'scripts': ['Shopify.', 'shopify_pay']
            },
            'magento': {
                'paths': ['/skin/', '/js/mage/', '/media/'],
                'meta': ['Magento'],
                'scripts': ['Mage.', 'Magento']
            },
            'ghost': {
                'paths': ['/ghost/', '/content/themes/'],
                'meta': ['Ghost'],
                'comments': ['Ghost']
            },
            'squarespace': {
                'paths': ['squarespace.com', 'squarespace-cdn.com'],
                'meta': ['Squarespace'],
                'scripts': ['squarespace']
            },
            'wix': {
                'paths': ['wix.com', 'wixstatic.com'],
                'meta': ['Wix'],
                'scripts': ['wix']
            }
        }

        # Popular WordPress theme patterns
        self.wordpress_theme_patterns = {
            'astra': ['/astra/', 'astra-theme'],
            'oceanwp': ['/oceanwp/', 'ocean-wp'],
            'generatepress': ['/generatepress/', 'generate-press'],
            'divi': ['/divi/', 'et-divi', 'elegantthemes'],
            'avada': ['/avada/', 'avada-theme'],
            'enfold': ['/enfold/', 'enfold-theme'],
            'x_theme': ['/x/', 'x-theme'],
            'jupiter': ['/jupiter/', 'jupiter-theme'],
            'betheme': ['/betheme/', 'be-theme'],
            'the7': ['/the7/', 'the7-theme'],
            'salient': ['/salient/', 'salient-theme'],
            'bridge': ['/bridge/', 'bridge-theme'],
            'flatsome': ['/flatsome/', 'flatsome-theme'],
            'woodmart': ['/woodmart/', 'woodmart-theme'],
            'porto': ['/porto/', 'porto-theme']
        }

        # CSS framework patterns
        self.framework_patterns = {
            'bootstrap': ['bootstrap', 'btn-', 'col-', 'container-fluid'],
            'foundation': ['foundation', 'callout', 'grid-container'],
            'bulma': ['bulma', 'is-', 'has-'],
            'tailwind': ['tailwind', 'bg-', 'text-', 'flex-'],
            'materialize': ['materialize', 'waves-effect', 'collection'],
            'semantic_ui': ['semantic', 'ui menu', 'ui button'],
            'pure_css': ['pure-', 'pure-menu', 'pure-form'],
            'skeleton': ['skeleton', 'container', 'twelve columns']
        }

        # Page builder patterns
        self.page_builder_patterns = {
            'elementor': ['elementor', 'elementor-element'],
            'visual_composer': ['vc_', 'wpb_'],
            'beaver_builder': ['fl-builder', 'fl-module'],
            'siteorigin': ['siteorigin', 'so-panel'],
            'gutenberg': ['wp-block-', 'has-background'],
            'oxygen': ['oxygen', 'ct-section'],
            'brizy': ['brz-', 'brizy'],
            'thrive_architect': ['thrv_', 'tve_'],
            'cornerstone': ['cornerstone', 'cs-content']
        }

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "third_party_services"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract third-party services and theme data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted third-party services and theme data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Initialize the services dictionary with required fields from the schema
            services = {
                'has_crazyegg': False,
                'has_calltrackingmetrics': False,
                'has_callrail': False,
                'has_bing_webmaster': False,
                'third_party_scripts': [],
                # Theme and CMS detection fields
                'detected_cms': None,
                'cms_version': None,
                'detected_theme': None,
                'theme_version': None,
                'detected_frameworks': [],
                'detected_page_builders': [],
                'wordpress_themes': [],
                'theme_indicators': []
            }

            # Extract all script tags
            script_tags = soup.find_all("script")

            # Analyze script tags to detect third-party services
            self._analyze_script_tags(script_tags, services, domain)

            # Look for iframe tags that might load third-party services
            iframe_tags = soup.find_all("iframe")
            self._analyze_iframe_tags(iframe_tags, services)

            # Check meta tags for specific services (e.g., Bing Webmaster Tools)
            self._analyze_meta_tags(soup, services)

            # Extract service-specific data based on our known patterns
            self._detect_known_services(script_tags, services)

            # Detect CMS and themes
            self._detect_cms_and_themes(soup, html_content, services, url)

            # Detect CSS frameworks
            self._detect_css_frameworks(soup, html_content, services)

            # Detect page builders
            self._detect_page_builders(soup, html_content, services)

            self.logger.debug(
                f"Extracted third_party_services: {services} for doc_id: {doc_id_str}"
            )

            return services

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _detect_cms_and_themes(self, soup, html_content, services, url):
        """Detect CMS platform and active themes.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
            url (str): The URL of the page.
        """
        # Check generator meta tag first
        generator_tag = soup.find("meta", attrs={"name": "generator"})
        if generator_tag:
            generator_content = generator_tag.get("content", "").lower()
            services['theme_indicators'].append(f"generator: {generator_content}")

            # WordPress detection
            if "wordpress" in generator_content:
                services['detected_cms'] = "WordPress"
                # Extract version if available
                version_match = re.search(r'wordpress\s+([\d.]+)', generator_content)
                if version_match:
                    services['cms_version'] = version_match.group(1)

            # Other CMS detection
            elif "drupal" in generator_content:
                services['detected_cms'] = "Drupal"
                version_match = re.search(r'drupal\s+([\d.]+)', generator_content)
                if version_match:
                    services['cms_version'] = version_match.group(1)

            elif "joomla" in generator_content:
                services['detected_cms'] = "Joomla"
                version_match = re.search(r'joomla!\s+([\d.]+)', generator_content)
                if version_match:
                    services['cms_version'] = version_match.group(1)

        # Check CSS and JS file paths for CMS and theme detection
        self._analyze_resource_paths(soup, services)

        # Check body classes for theme information
        self._analyze_body_classes(soup, services)

        # Check HTML comments for theme information
        self._analyze_html_comments(html_content, services)

        # WordPress-specific theme detection
        if services['detected_cms'] == "WordPress" or self._has_wordpress_indicators(soup, html_content):
            if not services['detected_cms']:
                services['detected_cms'] = "WordPress"
            self._detect_wordpress_themes(soup, html_content, services)

        # Shopify theme detection
        self._detect_shopify_themes(soup, html_content, services)

        # Generic theme detection from CSS files
        self._detect_theme_from_css_files(soup, services)

    def _analyze_resource_paths(self, soup, services):
        """Analyze CSS and JS file paths for CMS and theme indicators.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            services (dict): Dictionary to update with findings.
        """
        # Check CSS files
        css_links = soup.find_all("link", rel="stylesheet")
        for link in css_links:
            href = link.get("href", "")
            self._analyze_path_for_cms_theme(href, services)

        # Check JS files
        script_tags = soup.find_all("script", src=True)
        for script in script_tags:
            src = script.get("src", "")
            self._analyze_path_for_cms_theme(src, services)

    def _analyze_path_for_cms_theme(self, path, services):
        """Analyze a single path for CMS and theme indicators.

        Args:
            path (str): The file path to analyze.
            services (dict): Dictionary to update with findings.
        """
        if not path:
            return

        path_lower = path.lower()

        # WordPress theme detection
        wp_themes_match = re.search(r'/wp-content/themes/([^/]+)', path_lower)
        if wp_themes_match:
            theme_name = wp_themes_match.group(1)
            services['detected_cms'] = "WordPress"
            if theme_name not in services['wordpress_themes']:
                services['wordpress_themes'].append(theme_name)
            services['theme_indicators'].append(f"wp-theme-path: {theme_name}")

        # Check for specific WordPress themes
        for theme_name, patterns in self.wordpress_theme_patterns.items():
            if any(pattern in path_lower for pattern in patterns):
                if theme_name not in services['wordpress_themes']:
                    services['wordpress_themes'].append(theme_name)
                services['theme_indicators'].append(f"wp-theme-pattern: {theme_name}")

        # Drupal theme detection
        drupal_themes_match = re.search(r'/(?:sites/[^/]+/)?themes/([^/]+)', path_lower)
        if drupal_themes_match:
            theme_name = drupal_themes_match.group(1)
            services['detected_cms'] = "Drupal"
            services['detected_theme'] = theme_name
            services['theme_indicators'].append(f"drupal-theme: {theme_name}")

        # Joomla template detection
        joomla_template_match = re.search(r'/templates/([^/]+)', path_lower)
        if joomla_template_match:
            template_name = joomla_template_match.group(1)
            services['detected_cms'] = "Joomla"
            services['detected_theme'] = template_name
            services['theme_indicators'].append(f"joomla-template: {template_name}")

        # CMS detection from paths
        for cms, patterns in self.cms_patterns.items():
            if any(pattern in path_lower for pattern in patterns.get('paths', [])):
                if not services['detected_cms']:
                    services['detected_cms'] = cms.title()
                services['theme_indicators'].append(f"cms-path: {cms}")

    def _analyze_body_classes(self, soup, services):
        """Analyze body tag classes for theme information.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            services (dict): Dictionary to update with findings.
        """
        body = soup.find("body")
        if not body:
            return

        body_classes = body.get("class", [])
        if isinstance(body_classes, str):
            body_classes = body_classes.split()

        for class_name in body_classes:
            class_lower = class_name.lower()

            # WordPress theme classes
            if class_lower.startswith(('theme-', 'twentytwenty', 'twentynineteen')):
                services['detected_cms'] = "WordPress"
                services['theme_indicators'].append(f"body-class: {class_name}")

                # Extract theme name from class
                if class_lower.startswith('theme-'):
                    theme_name = class_lower.replace('theme-', '')
                    if theme_name not in services['wordpress_themes']:
                        services['wordpress_themes'].append(theme_name)

            # Check for specific theme patterns in body classes
            for theme_name, patterns in self.wordpress_theme_patterns.items():
                if any(pattern.replace('/', '').replace('-', '') in class_lower for pattern in patterns):
                    services['detected_cms'] = "WordPress"
                    if theme_name not in services['wordpress_themes']:
                        services['wordpress_themes'].append(theme_name)
                    services['theme_indicators'].append(f"body-class-theme: {theme_name}")

    def _analyze_html_comments(self, html_content, services):
        """Analyze HTML comments for theme information.

        Args:
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
        """
        # Find all HTML comments
        comment_pattern = r'<!--(.*?)-->'
        comments = re.findall(comment_pattern, html_content, re.DOTALL | re.IGNORECASE)

        for comment in comments:
            comment_lower = comment.lower().strip()

            # WordPress theme comments
            if 'theme' in comment_lower and ('wordpress' in comment_lower or 'wp' in comment_lower):
                services['detected_cms'] = "WordPress"
                services['theme_indicators'].append(f"comment: {comment_lower[:100]}")

                # Try to extract theme name
                theme_match = re.search(r'theme[:\s]+([^\s\n\r]+)', comment_lower)
                if theme_match:
                    theme_name = theme_match.group(1).strip('.,;:')
                    if theme_name not in services['wordpress_themes']:
                        services['wordpress_themes'].append(theme_name)

            # Generic theme information in comments
            if any(keyword in comment_lower for keyword in ['theme', 'template', 'design by']):
                services['theme_indicators'].append(f"comment: {comment_lower[:100]}")

    def _has_wordpress_indicators(self, soup, html_content):
        """Check if there are WordPress indicators in the content.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.

        Returns:
            bool: True if WordPress indicators are found.
        """
        # Check for wp- prefixed classes or IDs
        wp_elements = soup.find_all(attrs={"class": re.compile(r'\bwp-')})
        wp_elements.extend(soup.find_all(attrs={"id": re.compile(r'\bwp-')}))

        if wp_elements:
            return True

        # Check for WordPress-specific paths
        wp_paths = ['/wp-content/', '/wp-includes/', '/wp-admin/', 'wp-json']
        return any(path in html_content for path in wp_paths)

    def _detect_wordpress_themes(self, soup, html_content, services):
        """Detect WordPress themes from various indicators.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
        """
        # Look for theme-specific CSS classes and elements
        for theme_name, patterns in self.wordpress_theme_patterns.items():
            for pattern in patterns:
                if pattern in html_content.lower():
                    if theme_name not in services['wordpress_themes']:
                        services['wordpress_themes'].append(theme_name)
                    services['theme_indicators'].append(f"wp-theme-content: {theme_name}")

        # Set the primary detected theme if only one is found
        if len(services['wordpress_themes']) == 1:
            services['detected_theme'] = services['wordpress_themes'][0]
        elif len(services['wordpress_themes']) > 1:
            # Set the first one as primary
            services['detected_theme'] = services['wordpress_themes'][0]

    def _detect_shopify_themes(self, soup, html_content, services):
        """Detect Shopify themes and store information.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
        """
        # Check for Shopify indicators
        if any(indicator in html_content.lower() for indicator in ['shopify', 'cdn.shopify.com']):
            services['detected_cms'] = "Shopify"

            # Look for Shopify theme name in meta tags
            shopify_theme_meta = soup.find("meta", attrs={"name": "shopify-checkout-api-token"})
            if shopify_theme_meta:
                services['theme_indicators'].append("shopify-checkout-api")

            # Look for theme name in asset URLs
            theme_match = re.search(r'cdn\.shopify\.com/s/files/[^/]+/([^/]+)', html_content)
            if theme_match:
                theme_name = theme_match.group(1)
                services['detected_theme'] = theme_name
                services['theme_indicators'].append(f"shopify-theme: {theme_name}")

    def _detect_theme_from_css_files(self, soup, services):
        """Detect theme information from CSS file names and paths.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            services (dict): Dictionary to update with findings.
        """
        css_links = soup.find_all("link", rel="stylesheet")

        for link in css_links:
            href = link.get("href", "")
            if not href:
                continue

            # Extract filename from path
            filename = href.split('/')[-1].lower()

            # Look for theme indicators in filename
            if 'theme' in filename:
                services['theme_indicators'].append(f"css-file: {filename}")

                # Try to extract theme name
                theme_match = re.search(r'([a-zA-Z0-9-_]+)-?theme', filename)
                if theme_match and not services['detected_theme']:
                    services['detected_theme'] = theme_match.group(1)

    def _detect_css_frameworks(self, soup, html_content, services):
        """Detect CSS frameworks used on the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
        """
        html_lower = html_content.lower()

        for framework, patterns in self.framework_patterns.items():
            if any(pattern in html_lower for pattern in patterns):
                if framework not in services['detected_frameworks']:
                    services['detected_frameworks'].append(framework)

        # Check CSS file URLs for framework names
        css_links = soup.find_all("link", rel="stylesheet")
        for link in css_links:
            href = link.get("href", "").lower()
            for framework in self.framework_patterns.keys():
                if framework.replace('_', '-') in href or framework.replace('_', '') in href:
                    if framework not in services['detected_frameworks']:
                        services['detected_frameworks'].append(framework)

    def _detect_page_builders(self, soup, html_content, services):
        """Detect page builders used on the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            services (dict): Dictionary to update with findings.
        """
        html_lower = html_content.lower()

        for builder, patterns in self.page_builder_patterns.items():
            if any(pattern in html_lower for pattern in patterns):
                if builder not in services['detected_page_builders']:
                    services['detected_page_builders'].append(builder)

        # Check for page builder specific classes in DOM
        for builder, patterns in self.page_builder_patterns.items():
            for pattern in patterns:
                elements = soup.find_all(attrs={"class": re.compile(pattern, re.I)})
                if elements:
                    if builder not in services['detected_page_builders']:
                        services['detected_page_builders'].append(builder)

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
                        # Check for specific services first
                        if 'callrail.com' in src:
                            services['has_callrail'] = True
                            if 'CallRail' not in services['third_party_scripts']:
                                services['third_party_scripts'].append('CallRail')

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
            r'Drift\.',
            r'CallRail\.',
            r'callrail',
            r'cr\.call'
        ]

        for pattern in tracking_patterns:
            if re.search(pattern, script_content, re.IGNORECASE):
                service_name = self._identify_service_from_pattern(pattern, script_content)
                if service_name and service_name not in services['third_party_scripts']:
                    services['third_party_scripts'].append(service_name)

                    # Set specific service flags
                    if 'callrail' in pattern.lower():
                        services['has_callrail'] = True

    def _analyze_meta_tags(self, soup, services):
        """Analyze meta tags for third-party service verification codes.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            services (dict): Dictionary to update with findings.
        """
        # Check for Bing Webmaster Tools verification
        bing_meta_tags = soup.find_all("meta", attrs={"name": re.compile(r"msvalidate\.", re.I)})
        if bing_meta_tags:
            services['has_bing_webmaster'] = True
            if 'Bing Webmaster Tools' not in services['third_party_scripts']:
                services['third_party_scripts'].append('Bing Webmaster Tools')

        # Alternative Bing verification patterns
        bing_verification = soup.find("meta", attrs={"name": "msapplication-config"})
        if bing_verification:
            services['has_bing_webmaster'] = True
            if 'Bing Webmaster Tools' not in services['third_party_scripts']:
                services['third_party_scripts'].append('Bing Webmaster Tools')

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
        elif 'callrail' in pattern.lower() or 'cr.call' in pattern.lower():
            return 'CallRail'
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
                elif 'callrail.com' in src:
                    services['has_callrail'] = True
                    if 'CallRail' not in services['third_party_scripts']:
                        services['third_party_scripts'].append('CallRail')

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