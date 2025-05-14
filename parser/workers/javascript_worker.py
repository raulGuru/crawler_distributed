"""JavaScript Data Parser Worker module.

This module contains the JavascriptWorker class which extracts JavaScript usage
information from saved HTML files as part of a distributed crawl-parser system.
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


class JavascriptWorker(BaseParserWorker):
    """Worker for extracting JavaScript usage information from HTML files.

    This worker processes HTML files saved by the crawler, extracts various
    JavaScript-related elements and metrics including external scripts,
    frameworks, SPA indicators, and potential issues.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the JavascriptWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="htmlparser_javascript_extraction_tube",
            task_type="javascript_extraction",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "javascript_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract JavaScript-related data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted JavaScript data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract page URL for domain comparison
            page_url = self._extract_page_url(soup)
            current_domain = self._get_domain(page_url)

            # Extract file scripts
            file_scripts, file_script_count = self._extract_file_scripts(soup, current_domain)

            # Extract inline scripts
            inline_scripts = soup.find_all('script', src=False)
            inline_script_count = len(inline_scripts)
            inline_script_content = self._get_inline_script_content(inline_scripts)
            inline_script_size = len(inline_script_content)

            # Analyze script characteristics
            same_domain_script_count = sum(1 for script in file_scripts if script.get('is_same_domain', False))
            third_party_script_count = file_script_count - same_domain_script_count

            # Detect frameworks
            frameworks_detected = self._detect_frameworks(file_scripts, inline_script_content)

            # Detect SPA characteristics
            is_spa, spa_indicators = self._detect_spa(frameworks_detected, file_scripts, inline_script_content, page_url)

            # Find JS-dependent elements
            js_dependent_elements = self._find_js_dependent_elements(soup)

            # Check for lazy-loaded JS
            lazy_load_js_count = self._count_lazy_loaded_js(soup, inline_script_content)

            # Check for event listeners
            has_event_listeners = self._has_event_listeners(inline_script_content)

            # Identify potential issues
            issues = self._identify_issues(
                file_script_count,
                file_scripts,
                is_spa,
                inline_script_size
            )

            # Construct result dictionary based on MongoDB schema
            javascript_data = {
                "file_scripts": file_scripts,
                "file_script_count": file_script_count,
                "same_domain_script_count": same_domain_script_count,
                "third_party_script_count": third_party_script_count,
                "inline_script_count": inline_script_count,
                "inline_script_size": inline_script_size,
                "total_script_count": file_script_count + inline_script_count,
                "frameworks_detected": frameworks_detected,
                "is_spa": is_spa,
                "spa_indicators": spa_indicators,
                "js_dependent_elements": js_dependent_elements,
                "lazy_load_js_count": lazy_load_js_count,
                "has_event_listeners": has_event_listeners,
                "issues": issues
            }

            self.logger.debug(
                f"Extracted javascript_data for doc_id: {doc_id_str}"
            )

            return javascript_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_page_url(self, soup):
        """Extract the canonical URL of the page."""
        # Try canonical link first
        canonical = soup.find("link", rel="canonical")
        if canonical and canonical.get("href"):
            return canonical["href"]

        # Try Open Graph URL
        og_url = soup.find("meta", property="og:url")
        if og_url and og_url.get("content"):
            return og_url["content"]

        # Fallback to a reasonable default
        return "https://unknown-domain.com"

    def _get_domain(self, url):
        """Extract domain from URL."""
        try:
            parsed_url = urlparse(url)
            return parsed_url.netloc
        except Exception:
            return ""

    def _extract_file_scripts(self, soup, current_domain):
        """Extract external JavaScript files and their attributes.

        Args:
            soup: BeautifulSoup object
            current_domain: Domain of the current page

        Returns:
            tuple: (list of script objects, count of scripts)
        """
        file_scripts = []
        external_scripts = soup.find_all("script", src=True)

        for script in external_scripts:
            src = script.get("src", "")

            # Skip empty src
            if not src:
                continue

            # Create absolute URL if needed
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = f"https://{current_domain}{src}"

            # Extract script domain
            script_domain = self._get_domain(src)

            # Create script object
            script_data = {
                "src": src,
                "type": script.get("type"),
                "id": script.get("id"),
                "async": "async" in script.attrs,
                "defer": "defer" in script.attrs,
                "is_module": script.get("type") == "module",
                "domain": script_domain,
                "is_same_domain": script_domain == current_domain or not script_domain
            }

            file_scripts.append(script_data)

        return file_scripts, len(file_scripts)

    def _get_inline_script_content(self, inline_scripts):
        """Concatenate content from all inline scripts.

        Args:
            inline_scripts: List of inline script elements

        Returns:
            str: Combined content of all inline scripts
        """
        return '\n'.join([script.string or "" for script in inline_scripts])

    def _detect_frameworks(self, file_scripts, inline_script_content):
        """Detect JavaScript frameworks and libraries.

        Args:
            file_scripts: List of external script objects
            inline_script_content: Combined content of inline scripts

        Returns:
            list: Names of detected frameworks
        """
        frameworks = []

        # Check external script URLs for framework indicators
        framework_indicators = {
            "jquery": "jQuery",
            "react": "React",
            "angular": "Angular",
            "vue": "Vue.js",
            "ember": "Ember.js",
            "backbone": "Backbone.js",
            "knockout": "Knockout.js",
            "prototype": "Prototype.js",
            "mootools": "MooTools",
            "dojo": "Dojo",
            "gsap": "GSAP",
            "three.js": "Three.js",
            "d3": "D3.js",
            "leaflet": "Leaflet",
            "moment": "Moment.js"
        }

        # Check script sources
        for script in file_scripts:
            src = script.get("src", "").lower()
            for key, framework in framework_indicators.items():
                if key in src and framework not in frameworks:
                    frameworks.append(framework)

        # Check inline script content for framework signatures
        inline_content_lower = inline_script_content.lower()

        # jQuery detection
        if ("jquery" in inline_content_lower or
            "$(" in inline_content_lower or
            "$.ajax" in inline_content_lower):
            if "jQuery" not in frameworks:
                frameworks.append("jQuery")

        # React detection
        if ("react" in inline_content_lower or
            "reactdom" in inline_content_lower or
            "createelement" in inline_content_lower or
            "jsx" in inline_content_lower):
            if "React" not in frameworks:
                frameworks.append("React")

        # Angular detection
        if ("angular" in inline_content_lower or
            "ng-app" in inline_content_lower or
            "ng-controller" in inline_content_lower):
            if "Angular" not in frameworks:
                frameworks.append("Angular")

        # Vue detection
        if ("vue" in inline_content_lower or
            "new vue" in inline_content_lower or
            "v-bind" in inline_content_lower or
            "v-model" in inline_content_lower or
            "v-if" in inline_content_lower):
            if "Vue.js" not in frameworks:
                frameworks.append("Vue.js")

        return frameworks

    def _detect_spa(self, frameworks_detected, file_scripts, inline_script_content, page_url):
        """Detect if the page is likely a Single Page Application.

        Args:
            frameworks_detected: List of detected frameworks
            file_scripts: List of external script objects
            inline_script_content: Combined content of inline scripts
            page_url: URL of the page

        Returns:
            tuple: (is_spa boolean, list of SPA indicators)
        """
        spa_indicators = []

        # SPA frameworks
        spa_frameworks = ["React", "Angular", "Vue.js", "Ember.js"]
        for framework in spa_frameworks:
            if framework in frameworks_detected:
                spa_indicators.append(f"SPA framework detected ({framework})")

        # Check for routing libraries
        for script in file_scripts:
            src = script.get("src", "").lower()
            if any(router in src for router in ["router", "routing", "history.js"]):
                spa_indicators.append("Client-side routing library detected")
                break

        # Check for SPA patterns in inline scripts
        if "renderroute" in inline_script_content.lower() or "renderview" in inline_script_content.lower():
            spa_indicators.append("Client-side routing code detected")

        # Check for hash-based navigation
        if '#!' in page_url or '/#/' in page_url:
            spa_indicators.append("Hash-based navigation in URL")

        # If there's evidence of an SPA, return True
        is_spa = len(spa_indicators) > 0

        return is_spa, spa_indicators

    def _find_js_dependent_elements(self, soup):
        """Find elements that are likely dependent on JavaScript.

        Args:
            soup: BeautifulSoup object

        Returns:
            list: Objects describing JS-dependent elements
        """
        js_dependent_elements = []

        # Common JS-dependent selectors to look for
        js_selectors = [
            "[onclick]",
            "[onload]",
            "[onchange]",
            "[v-if]",
            "[v-for]",
            "[v-bind]",
            "[v-model]",
            "[ng-if]",
            "[ng-repeat]",
            "[ng-bind]",
            "[ng-model]",
            "[data-reactid]",
            "[jsx]",
            "[data-vue]",
            "[x-data]",  # Alpine.js
            "[data-controller]",  # Stimulus
            ".js-",  # Common pattern for JS-targeted elements
            "[data-toggle]",  # Bootstrap
            "[data-target]"   # Bootstrap
        ]

        # Count occurrences of each selector
        for selector in js_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    js_dependent_elements.append({
                        "selector": selector,
                        "count": len(elements)
                    })
            except Exception:
                # Skip selectors that can't be processed by the parser
                continue

        return js_dependent_elements

    def _count_lazy_loaded_js(self, soup, inline_script_content):
        """Count JavaScript files that are loaded lazily.

        Args:
            soup: BeautifulSoup object
            inline_script_content: Combined content of inline scripts

        Returns:
            int: Count of lazy-loaded JS files
        """
        lazy_load_count = 0

        # Check for dynamic script creation in inline scripts
        if (
            "document.createElement('script')" in inline_script_content or
            'document.createElement("script")' in inline_script_content or
            "loadScript" in inline_script_content or
            "appendChild" in inline_script_content and "script" in inline_script_content
        ):
            # Estimate based on pattern matches - this is approximate
            patterns = [
                "document.createElement('script')",
                'document.createElement("script")',
                "loadScript(",
                "loadJS("
            ]

            for pattern in patterns:
                lazy_load_count += inline_script_content.count(pattern)

        # Check for script elements with data-src (common lazy loading pattern)
        lazy_load_count += len(soup.select('script[data-src]'))

        # Check for script elements with defer attribute (technically not lazy loading but delayed)
        # Only count here if we didn't find other lazy loading indicators
        if lazy_load_count == 0:
            deferred_scripts = soup.select('script[defer]')
            lazy_load_count = len(deferred_scripts)

        return lazy_load_count

    def _has_event_listeners(self, inline_script_content):
        """Check if the page has event listeners.

        Args:
            inline_script_content: Combined content of inline scripts

        Returns:
            bool: True if event listeners are detected
        """
        event_listener_patterns = [
            'addEventListener(',
            'attachEvent(',
            '.on',
            'onclick',
            'onload',
            'onscroll',
            'onresize',
            'onchange',
            'oninput',
            'onmouseover',
            'onmouseout',
            'onkeyup',
            'onkeydown',
            'onsubmit',
            'onselect',
            'onfocus',
            'onblur'
        ]

        for pattern in event_listener_patterns:
            if pattern in inline_script_content:
                return True

        return False

    def _identify_issues(self, file_script_count, file_scripts, is_spa, inline_script_size):
        """Identify potential issues with JavaScript implementation.

        Args:
            file_script_count: Number of external script files
            file_scripts: List of external script objects
            is_spa: Whether the page is a SPA
            inline_script_size: Size of inline scripts in bytes

        Returns:
            list: Identified issues
        """
        issues = []

        # Check for excessive file scripts
        if file_script_count > 15:  # Arbitrary threshold
            issues.append("excessive_file_scripts")

        # Check for large inline scripts
        if inline_script_size > 100000:  # 100KB, arbitrary threshold
            issues.append("large_inline_scripts")

        # Check for missing async/defer attributes
        scripts_without_async_defer = [s for s in file_scripts
                                      if not s.get("async") and not s.get("defer")]
        if scripts_without_async_defer:
            issues.append("missing_async_defer")

        # Check for SPA without server-side rendering indications
        if is_spa:
            # Look for common SSR indicators in scripts
            ssr_indicators = any("SSR" in s.get("src", "") or "server-side" in s.get("src", "")
                                for s in file_scripts)

            if not ssr_indicators:
                issues.append("spa_without_ssr_indicators")

        # Check for jQuery without CDN
        has_jquery = any("jQuery" in s.get("src", "") for s in file_scripts)
        if has_jquery and not any("cdn" in s.get("src", "").lower() for s in file_scripts):
            issues.append("jquery_without_cdn")

        # Check for minified scripts
        if not any(".min.js" in s.get("src", "") for s in file_scripts) and file_script_count > 2:
            issues.append("unminified_scripts")

        return issues


def main():
    """Main entry point for the JavaScript Data Parser Worker."""
    parser = argparse.ArgumentParser(description="JavaScript Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = JavascriptWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()