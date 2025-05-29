"""Canonical URL Parser Worker module.

This module contains the CanonicalWorker class which extracts canonical URL information
from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import argparse
from urllib.parse import urljoin
import re

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class CanonicalWorker(BaseParserWorker):
    """Worker for extracting canonical URL information from HTML files.

    This worker processes HTML files saved by the crawler, extracts canonical URL
    information including HTML link elements and HTTP headers, analyzes relationships
    between canonical URLs and the current page URL, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the CanonicalWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_canonical_tube",
            task_type="canonical",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "canonical_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract canonical URL information from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted canonical data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            current_url = url or self._get_current_url_from_file(html_path)

            # Load headers from file
            headers_file_path = self.job_data.get('headers_file_path')
            response_headers = self._load_headers_from_file(headers_file_path)
            if not response_headers:
                self.logger.warning(f"headers_file_path not found for doc_id {doc_id_str}")
                response_headers = {}
            else:
                self.logger.warning(f"headers_file_path not found for doc_id {doc_id_str}")
                response_headers = {}

            # Extract canonical URL from HTML link element
            canonical_tags = soup.find_all("link", rel="canonical")
            canonical_url = None
            has_multiple_canonicals = len(canonical_tags) > 1

            # Get the primary canonical URL (if multiple, use the first one)
            if canonical_tags:
                canonical_url = canonical_tags[0].get("href")
                if canonical_url and not canonical_url.startswith(('http://', 'https://')):
                    # Handle relative URLs by making them absolute
                    if current_url:
                        canonical_url = urljoin(current_url, canonical_url)

            # Extract HTTP headers canonical from response headers
            http_canonical = None
            link_header_values = response_headers.get('link', [])

            if not isinstance(link_header_values, list):
                link_header_values = [link_header_values]

            for link_header_line in link_header_values:
                if link_header_line:
                    for link_part in link_header_line.split(','):
                        if 'rel="canonical"' in link_part.lower() or "rel='canonical'" in link_part.lower():
                            match = re.search(r'<([^>]+)>', link_part)
                            if match:
                                http_canonical = match.group(1)
                                break
                if http_canonical:
                    break

            # Check if we have any form of canonical
            has_canonical = bool(canonical_url) or bool(http_canonical)

            # Check if canonical URL points to the current page
            is_self_canonical = False
            if canonical_url and current_url:
                is_self_canonical = self._normalize_url(canonical_url) == self._normalize_url(current_url)

            # Check if page is being canonicalized elsewhere (canonical exists but not pointing to self)
            # TODO: Will be added later
            # is_canonicalized = has_canonical and not is_self_canonical

            # Check for conflicts between HTML and HTTP header canonicals
            # TODO: Will be added later
            #has_canonical_conflict = canonical_url and http_canonical and canonical_url != http_canonical

            # Collect all canonical declarations
            all_canonical_tags = []
            if canonical_url:
                for tag in canonical_tags:
                    url = tag.get("href")
                    if url:
                        # Handle relative URLs
                        if not url.startswith(('http://', 'https://')) and current_url:
                            url = urljoin(current_url, url)

                        is_tag_self_canonical = False
                        if current_url:
                            is_tag_self_canonical = self._normalize_url(url) == self._normalize_url(current_url)

                        all_canonical_tags.append({
                            "url": url,
                            "source": "HTML link element",
                            "is_self_canonical": is_tag_self_canonical
                        })

            if http_canonical:
                all_canonical_tags.append({
                    "url": http_canonical,
                    "source": "HTTP header",
                    "is_self_canonical": current_url and self._normalize_url(http_canonical) == self._normalize_url(current_url)
                })

            # Analyze potential issues
            issues = []

            # Missing canonical on HTML page
            if not has_canonical:
                issues.append("missing_canonical")

            # Multiple canonical tags
            if has_multiple_canonicals:
                issues.append("multiple_canonicals")

            # Canonical conflict
            # TODO: Will be added later
            # if has_canonical_conflict:
            #     issues.append("canonical_conflict")

            # Check for robots meta tag with noindex
            meta_robots = soup.find("meta", attrs={"name": "robots"})
            robots_content = meta_robots.get("content", "") if meta_robots else ""

            # Get X-Robots-Tag from headers
            x_robots_tag_values = response_headers.get('x-robots-tag', [])
            x_robots_tag = ""
            if x_robots_tag_values:
                x_robots_tag = ", ".join(x_robots_tag_values)

            is_noindex = ('noindex' in robots_content.lower() or 'noindex' in x_robots_tag.lower())

            # Noindex with canonical (usually not recommended)
            if is_noindex and has_canonical:
                issues.append("noindex_with_canonical")

            # Build the canonical data structure
            canonical_data = {
                "canonical_url": canonical_url,
                "http_canonical": http_canonical,
                "has_canonical": has_canonical,
                "is_self_canonical": is_self_canonical,
                #"is_canonicalized": is_canonicalized,  # TODO: Will be added later
                "has_multiple_canonicals": has_multiple_canonicals,
                #"has_canonical_conflict": has_canonical_conflict if (canonical_url and http_canonical) else None,  # TODO: Will be added later
                "all_canonical_tags": all_canonical_tags,
                "issues": issues
            }

            self.logger.debug(
                f"Extracted canonical_data: {canonical_data} for doc_id: {doc_id_str}"
            )

            return canonical_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for comparison.

        Args:
            url (str): URL to normalize.

        Returns:
            str: Normalized URL.
        """
        if not url:
            return ""

        # Convert to lowercase
        url = url.lower()

        # Remove trailing slash if present
        if url.endswith('/'):
            url = url[:-1]

        # Remove protocol (http://, https://)
        if url.startswith('http://'):
            url = url[7:]
        elif url.startswith('https://'):
            url = url[8:]

        # Remove www. if present
        if url.startswith('www.'):
            url = url[4:]

        return url

    def _get_current_url_from_file(self, html_path: str) -> str:
        """Extract or infer the current URL from file metadata.

        Args:
            html_path (str): Path to the HTML file.

        Returns:
            str: The current URL or None if it can't be determined.
        """
        # Ideally, this information would come from job metadata
        # For now, we'll try to extract from filename or return None

        try:
            # This is a very basic approach and may need to be customized
            # based on specific file naming convention
            filename = os.path.basename(html_path)

            # If filename contains URL-like structure (e.g., "example_com_page_html")
            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 2:
                    domain = parts[0].replace('https', '').replace('http', '')
                    if not domain.startswith(('://', '/')):
                        domain = f"https://{domain}"
                    path = '/'.join(parts[1:]).replace('html', '.html')
                    return f"{domain}/{path}"

            # If we can't infer URL, return None
            return None

        except Exception as e:
            self.logger.warning(f"Failed to extract URL from filename: {e}")
            return None


def main():
    """Main entry point for the Canonical URL Parser Worker."""
    parser = argparse.ArgumentParser(description="Canonical URL Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = CanonicalWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()