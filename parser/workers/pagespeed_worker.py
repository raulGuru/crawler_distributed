"""Page Speed Parser Worker module.

This module contains the PageSpeedWorker class which extracts performance metrics
from saved HTML files as part of a distributed crawl-parser system.
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


class PageSpeedWorker(BaseParserWorker):
    """Worker for extracting page speed and performance metrics from HTML files.

    This worker processes HTML files saved by the crawler, extracts performance metrics
    such as page size, resource counts, render-blocking resources, and optimization
    opportunities, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the PageSpeedWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_page_speed_tube",
            task_type="page_speed",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "pagespeed_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract page speed and performance metrics from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted page speed data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Get response time from job metadata (if available)
            response_time_seconds = None
            headers = self._load_headers_from_file(self.job_data['headers_file_path'])
            if headers:
                response_time_seconds = headers.get('download_latency', '')

            # Calculate page size
            page_size_bytes = len(html_content)
            page_size_kb = round(page_size_bytes / 1024, 2)

            # Determine if content is compressed (estimate from headers)
            transferred_bytes = 0
            compressed = False
            compression_ratio = 0

            if headers:
                content_encoding = headers.get('content-encoding', '')
                compressed = 'gzip' in content_encoding or 'deflate' in content_encoding or 'br' in content_encoding
                content_length = headers.get('content-length', '')
                if content_length and content_length.isdigit():
                    transferred_bytes = int(content_length)
                    if transferred_bytes > 0 and compressed:
                        compression_ratio = round(page_size_bytes / transferred_bytes, 2)

            # Extract and analyze resources
            resource_counts, resources, render_blocking = self._analyze_resources(soup, domain)

            # Check for performance indicators
            performance_indicators = self._analyze_performance_indicators(
                soup, html_content, resources, render_blocking
            )

            # Identify optimization opportunities
            optimization_opportunities = self._identify_optimization_opportunities(
                resources, performance_indicators, response_time_seconds
            )

            # Identify performance issues
            issues = self._identify_issues(performance_indicators, response_time_seconds)

            # Count DOM elements
            dom_elements = len(soup.find_all())

            # Build pagespeed data structure
            pagespeed_data = {
                "url": url,
                "response_time_seconds": response_time_seconds,
                "page_size_bytes": page_size_bytes,
                "page_size_kb": page_size_kb,
                "transferred_bytes": transferred_bytes,
                "compressed": compressed,
                "compression_ratio": compression_ratio,
                "resource_counts": resource_counts,
                "render_blocking": render_blocking,
                "performance_indicators": performance_indicators,
                "optimization_opportunities": optimization_opportunities,
                "resources": resources,
                "issues": issues,
                "dom_elements": dom_elements,
                "has_critical_issues": len(issues) > 0,
                "optimization_count": len(optimization_opportunities),
                "note": "Performance metrics extracted from static HTML analysis"
            }

            self.logger.debug(
                f"Extracted pagespeed_data for doc_id: {doc_id_str}"
            )

            return pagespeed_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _analyze_resources(self, soup, domain):
        """Analyze resources (JS, CSS, images, etc.) on the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            domain (str): Domain of the current page.

        Returns:
            tuple: (resource_counts, resources, render_blocking)
        """
        # Initialize resource counts
        resource_counts = {
            "total": 0,
            "js": 0,
            "css": 0,
            "images": 0,
            "fonts": 0,
            "videos": 0,
            "iframes": 0,
            "third_party": 0
        }

        # Initialize render blocking counts
        render_blocking = {
            "js": 0,
            "css": 0
        }

        # Detailed list of resources
        resources = []

        # Extract JavaScript files
        js_files = soup.find_all("script", src=True)
        resource_counts["js"] = len(js_files)

        for script in js_files:
            script_src = script.get("src", "")
            script_url = script_src
            script_domain = urlparse(script_url).netloc

            # Determine if third-party
            is_third_party = script_domain and script_domain != domain
            if is_third_party:
                resource_counts["third_party"] += 1

            # Determine if render-blocking
            is_render_blocking = not script.get("async") and not script.get("defer")
            if is_render_blocking:
                render_blocking["js"] += 1

            resources.append({
                "url": script_url,
                "type": "js",
                "third_party": is_third_party,
                "render_blocking": is_render_blocking
            })

        # Extract CSS files
        css_files = soup.find_all("link", rel="stylesheet")
        resource_counts["css"] = len(css_files)

        for css in css_files:
            css_href = css.get("href", "")
            css_url = css_href
            css_domain = urlparse(css_url).netloc

            # Determine if third-party
            is_third_party = css_domain and css_domain != domain
            if is_third_party:
                resource_counts["third_party"] += 1

            # Determine if render-blocking (most CSS is render-blocking by default)
            is_render_blocking = True
            media_attr = css.get("media", "")
            if media_attr and "print" in media_attr:
                is_render_blocking = False

            if is_render_blocking:
                render_blocking["css"] += 1

            resources.append({
                "url": css_url,
                "type": "css",
                "third_party": is_third_party,
                "render_blocking": is_render_blocking
            })

        # Extract images
        images = soup.find_all("img")
        resource_counts["images"] = len(images)

        for img in images:
            img_src = img.get("src") or img.get("data-src", "")
            if img_src:
                img_url = img_src
                img_domain = urlparse(img_url).netloc

                # Determine if third-party
                is_third_party = img_domain and img_domain != domain
                if is_third_party:
                    resource_counts["third_party"] += 1

                # Check for image optimization attributes
                width = img.get("width")
                height = img.get("height")
                has_dimensions = width is not None and height is not None

                # Check for lazy loading
                is_lazy_loaded = (
                    img.get("loading") == "lazy" or
                    img.get("data-src") is not None or
                    img.get("data-lazy-src") is not None
                )

                # Check for responsive images
                is_responsive = img.get("srcset") is not None

                resources.append({
                    "url": img_url,
                    "type": "image",
                    "third_party": is_third_party,
                    "has_dimensions": has_dimensions,
                    "lazy_loaded": is_lazy_loaded,
                    "responsive": is_responsive
                })

        # Extract iframes
        iframes = soup.find_all("iframe")
        resource_counts["iframes"] = len(iframes)

        for iframe in iframes:
            iframe_src = iframe.get("src", "")
            if iframe_src:
                iframe_url = iframe_src
                iframe_domain = urlparse(iframe_url).netloc

                # Determine if third-party
                is_third_party = iframe_domain and iframe_domain != domain
                if is_third_party:
                    resource_counts["third_party"] += 1

                # Check for lazy loading
                is_lazy_loaded = iframe.get("loading") == "lazy"

                resources.append({
                    "url": iframe_url,
                    "type": "iframe",
                    "third_party": is_third_party,
                    "lazy_loaded": is_lazy_loaded
                })

        # Extract font files
        font_links = soup.find_all("link", href=lambda href: href and any(ext in href for ext in [".woff", ".woff2", ".ttf", ".otf", ".eot"]))
        resource_counts["fonts"] = len(font_links)

        for font in font_links:
            font_href = font.get("href", "")
            font_url = font_href
            font_domain = urlparse(font_url).netloc

            # Determine if third-party
            is_third_party = font_domain and font_domain != domain
            if is_third_party:
                resource_counts["third_party"] += 1

            resources.append({
                "url": font_url,
                "type": "font",
                "third_party": is_third_party
            })

        # Extract video elements
        videos = soup.find_all("video")
        resource_counts["videos"] = len(videos)

        for video in videos:
            source_tags = video.find_all("source")
            for source in source_tags:
                video_src = source.get("src", "")
                video_url = video_src
                video_domain = urlparse(video_url).netloc

                # Determine if third-party
                is_third_party = video_domain and video_domain != domain
                if is_third_party:
                    resource_counts["third_party"] += 1

                resources.append({
                    "url": video_url,
                    "type": "video",
                    "third_party": is_third_party
                })

        # Calculate total resources
        resource_counts["total"] = (
            resource_counts["js"] +
            resource_counts["css"] +
            resource_counts["images"] +
            resource_counts["fonts"] +
            resource_counts["videos"] +
            resource_counts["iframes"]
        )

        return resource_counts, resources, render_blocking

    def _analyze_performance_indicators(self, soup, html_content, resources, render_blocking):
        """Analyze performance indicators for the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_content (str): Raw HTML content.
            resources (list): List of resource objects.
            render_blocking (dict): Render blocking counts.

        Returns:
            dict: Performance indicators.
        """
        # Initialize performance indicators
        indicators = {
            "has_minified_css": False,
            "has_minified_js": False,
            "has_render_blocking_resources": False,
            "has_unoptimized_images": False,
            "has_excessive_dom_size": False,
            "has_large_network_payloads": False
        }

        # Check for minified CSS
        style_tags = soup.find_all("style")
        if style_tags:
            css_content = "\n".join([tag.string or "" for tag in style_tags])
            # Heuristic: minified CSS typically has very few newlines relative to its length
            indicators["has_minified_css"] = (
                len(css_content.splitlines()) <= 5 and
                len(css_content) > 500
            )

        # Check for minified JS (similar heuristic)
        script_tags = soup.find_all("script", src=False)
        if script_tags:
            js_content = "\n".join([tag.string or "" for tag in script_tags])
            # Look for typical patterns in minified JS
            indicators["has_minified_js"] = (
                len(js_content.splitlines()) <= 5 and
                len(js_content) > 500
            ) or ".min.js" in html_content

        # Check for render-blocking resources
        indicators["has_render_blocking_resources"] = (
            render_blocking["js"] > 0 or render_blocking["css"] > 0
        )

        # Check for unoptimized images
        unoptimized_images = [
            resource for resource in resources
            if resource["type"] == "image" and (
                not resource.get("has_dimensions") or
                not resource.get("lazy_loaded") or
                not resource.get("responsive")
            )
        ]
        indicators["has_unoptimized_images"] = len(unoptimized_images) > 0

        # Check for excessive DOM size
        dom_elements = len(soup.find_all())
        indicators["has_excessive_dom_size"] = dom_elements > 1500

        # Check for large network payloads
        total_resources = len(resources)
        indicators["has_large_network_payloads"] = total_resources > 50

        return indicators

    def _identify_optimization_opportunities(self, resources, indicators, response_time):
        """Identify optimization opportunities based on performance analysis.

        Args:
            resources (list): List of resource objects.
            indicators (dict): Performance indicators.
            response_time (float): Response time in seconds.

        Returns:
            list: Optimization opportunities.
        """
        opportunities = []

        # Check for slow server response time
        if response_time and response_time > 0.5:
            opportunities.append("Improve server response time")

        # Check for render-blocking resources
        if indicators["has_render_blocking_resources"]:
            opportunities.append("Eliminate render-blocking resources")

        # Check for unminified CSS
        if not indicators["has_minified_css"]:
            opportunities.append("Minify CSS")

        # Check for unminified JS
        if not indicators["has_minified_js"]:
            opportunities.append("Minify JavaScript")

        # Check for images without dimensions
        images_without_dimensions = [
            resource for resource in resources
            if resource["type"] == "image" and not resource.get("has_dimensions")
        ]
        if images_without_dimensions:
            opportunities.append("Properly size images")

        # Check for images without lazy loading
        non_lazy_images = [
            resource for resource in resources
            if resource["type"] == "image" and not resource.get("lazy_loaded")
        ]
        if non_lazy_images:
            opportunities.append("Defer offscreen images")

        # Check for excessive DOM size
        if indicators["has_excessive_dom_size"]:
            opportunities.append("Reduce DOM size")

        # Add generic optimization for many resources
        if indicators["has_large_network_payloads"]:
            opportunities.append("Reduce total resources")
            opportunities.append("Serve static assets with efficient cache policy")

        # Check if there are too many JS resources
        js_resources = [resource for resource in resources if resource["type"] == "js"]
        if len(js_resources) > 15:
            opportunities.append("Reduce JavaScript bundle size")

        return opportunities

    def _identify_issues(self, indicators, response_time):
        """Identify performance issues based on indicators.

        Args:
            indicators (dict): Performance indicators.
            response_time (float): Response time in seconds.

        Returns:
            list: Performance issues.
        """
        issues = []

        # Server response issues
        if response_time and response_time > 1.0:
            issues.append("slow_server_response_time")

        # Render-blocking issues
        if indicators["has_render_blocking_resources"]:
            issues.append("render_blocking_resources")

        # Image optimization issues
        if indicators["has_unoptimized_images"]:
            issues.append("unoptimized_images")

        # DOM size issues
        if indicators["has_excessive_dom_size"]:
            issues.append("excessive_dom_size")

        # Network payload issues
        if indicators["has_large_network_payloads"]:
            issues.append("large_network_payloads")

        # Minification issues
        if not indicators["has_minified_css"]:
            issues.append("unminified_css")

        if not indicators["has_minified_js"]:
            issues.append("unminified_js")

        # Generic issue for missing cache headers
        issues.append("missing_cache_headers")

        return issues


def main():
    """Main entry point for the Page Speed Parser Worker."""
    parser = argparse.ArgumentParser(description="Page Speed Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = PageSpeedWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()