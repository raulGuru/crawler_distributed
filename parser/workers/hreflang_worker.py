"""Hreflang Parser Worker module.

This module contains the HreflangWorker class which extracts hreflang
information from saved HTML files as part of a distributed crawl-parser system.
"""

# TODO:
# Extract the current page URL from HTML if possible
# Note: In a production system, this would likely come from job metadata

import os
import sys
import re
import argparse
from urllib.parse import urljoin

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class HreflangWorker(BaseParserWorker):
    """Worker for extracting hreflang information from HTML files.

    This worker processes HTML files saved by the crawler, extracts
    hreflang annotations from link elements and headers, analyzes them
    for completeness and correctness, and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the HreflangWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="htmlparser_hreflang_extraction_tube",
            task_type="hreflang_extraction",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "hreflang_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str) -> dict:
        """Extract hreflang data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted hreflang data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract the current page URL from HTML if possible
            # Note: In a production system, this would likely come from job metadata
            current_url = self._extract_page_url(soup)

            # Extract HTML hreflang tags
            html_hreflang_tags = self._extract_html_hreflang_tags(soup, current_url)

            # Extract HTTP header hreflang tags
            # In a saved HTML context, we need to look for saved header information
            http_hreflang_tags = self._extract_http_hreflang_tags(soup)

            # Combine all hreflang tags
            all_hreflang_tags = html_hreflang_tags + http_hreflang_tags

            # Check for self-referencing hreflang
            has_self_reference = self._check_self_reference(all_hreflang_tags, current_url)

            # Check for x-default
            has_x_default = any(tag["lang"] == "x-default" for tag in all_hreflang_tags)

            # Get all languages defined
            languages = [tag["lang"] for tag in all_hreflang_tags]
            unique_languages = list(set(languages))

            # Check for conflicts between HTML and HTTP implementations
            has_conflicts = self._check_for_conflicts(html_hreflang_tags, http_hreflang_tags)

            # Check for invalid language codes
            invalid_lang_codes = self._validate_language_codes(all_hreflang_tags)

            # Identify potential issues
            issues = self._identify_issues(
                all_hreflang_tags,
                has_self_reference,
                has_x_default,
                has_conflicts,
                invalid_lang_codes,
                languages,
                current_url
            )

            # Build final hreflang data structure
            hreflang_data = {
                "html_hreflang_tags": html_hreflang_tags,
                "http_hreflang_tags": http_hreflang_tags,
                "all_hreflang_tags": all_hreflang_tags,
                "has_self_reference": has_self_reference,
                "has_x_default": has_x_default,
                "languages": unique_languages,
                "total_languages": len(unique_languages),
                "has_conflicts": has_conflicts,
                "invalid_lang_codes": invalid_lang_codes,
                "issues": issues
            }

            self.logger.debug(
                f"Extracted hreflang data for doc_id: {doc_id_str}: "
                f"{len(all_hreflang_tags)} tags, {len(issues)} issues"
            )

            return hreflang_data

        except Exception as e:
            self.logger.error(
                f"Failed to extract hreflang data for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for hreflang extraction: {e}")

    def _extract_page_url(self, soup):
        """Extract the current page URL from HTML content.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            str: Page URL or empty string if not found.
        """
        # Try canonical URL first
        canonical = soup.find("link", rel="canonical")
        if canonical and canonical.get("href"):
            return canonical.get("href")

        # Try Open Graph URL
        og_url = soup.find("meta", property="og:url")
        if og_url and og_url.get("content"):
            return og_url.get("content")

        # Try base href
        base = soup.find("base", href=True)
        if base and base.get("href"):
            return base.get("href")

        return ""

    def _extract_html_hreflang_tags(self, soup, current_url):
        """Extract hreflang tags from HTML link elements.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            current_url (str): Current page URL for resolving relative URLs.

        Returns:
            list: List of hreflang tag objects.
        """
        html_hreflang_tags = []

        for tag in soup.find_all("link", rel="alternate", hreflang=True):
            lang = tag.get("hreflang")
            href = tag.get("href")

            if lang and href:
                # Convert relative URLs to absolute
                if current_url and not href.startswith(("http://", "https://")):
                    href = urljoin(current_url, href)

                html_hreflang_tags.append({
                    "lang": lang,
                    "href": href,
                    "source": "HTML link element"
                })

        return html_hreflang_tags

    def _extract_http_hreflang_tags(self, soup):
        """Extract hreflang tags from HTTP headers (if saved in HTML).

        In a real-world scenario, HTTP headers would come from the response.
        For saved HTML, we're looking for specifically saved header information.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            list: List of hreflang tag objects from HTTP headers.
        """
        http_hreflang_tags = []

        # Look for saved HTTP headers in HTML comments or meta tags
        # This is a simple implementation - adapt as needed for your specific system
        http_headers_comment = soup.find(string=lambda text: isinstance(text, type(soup.new_string("")))
                                        and "HTTP Headers" in text
                                        and "Link:" in text)

        if http_headers_comment:
            # Extract Link header with hreflang info
            link_match = re.search(r'Link:\s*(.+?)(?:\n|$)', http_headers_comment)
            if link_match:
                link_header = link_match.group(1)
                # Parse Link header with format: <url>; rel="alternate"; hreflang="lang"
                for link_part in link_header.split(','):
                    if 'rel="alternate"' in link_part and 'hreflang=' in link_part:
                        url_match = re.search(r'<([^>]+)>', link_part)
                        lang_match = re.search(r'hreflang="([^"]+)"', link_part)

                        if url_match and lang_match:
                            http_hreflang_tags.append({
                                "lang": lang_match.group(1),
                                "href": url_match.group(1),
                                "source": "HTTP header"
                            })

        return http_hreflang_tags

    def _check_self_reference(self, all_tags, current_url):
        """Check if hreflang tags include a self-reference.

        Args:
            all_tags (list): List of all hreflang tags.
            current_url (str): Current page URL.

        Returns:
            bool: True if self-reference exists, False otherwise.
        """
        if not current_url:
            return False

        # Normalize URLs for comparison (remove trailing slashes, etc.)
        normalized_current = self._normalize_url(current_url)

        # Check if any tag references the current URL
        for tag in all_tags:
            normalized_href = self._normalize_url(tag["href"])
            if normalized_href == normalized_current:
                return True

        return False

    def _normalize_url(self, url):
        """Normalize URL for comparison by removing trailing slash and fragment.

        Args:
            url (str): URL to normalize.

        Returns:
            str: Normalized URL.
        """
        if not url:
            return ""

        # Remove fragment
        url = url.split("#")[0]

        # Remove trailing slash if present
        if url.endswith("/"):
            url = url[:-1]

        return url.lower()

    def _check_for_conflicts(self, html_tags, http_tags):
        """Check for conflicts between HTML and HTTP implementation.

        Args:
            html_tags (list): HTML hreflang tags.
            http_tags (list): HTTP header hreflang tags.

        Returns:
            bool: True if conflicts exist, False otherwise.
        """
        # If either list is empty, there can't be conflicts
        if not html_tags or not http_tags:
            return False

        # Create sets of (lang, href) tuples for comparison
        html_pairs = {(self._normalize_url(tag["href"]), tag["lang"]) for tag in html_tags}
        http_pairs = {(self._normalize_url(tag["href"]), tag["lang"]) for tag in http_tags}

        # Check for conflicts - same language pointing to different URLs
        html_langs = {}
        for href, lang in html_pairs:
            if lang in html_langs and html_langs[lang] != href:
                return True
            html_langs[lang] = href

        http_langs = {}
        for href, lang in http_pairs:
            if lang in http_langs and http_langs[lang] != href:
                return True
            http_langs[lang] = href

        # Check for conflicts between HTML and HTTP
        for lang, href in html_langs.items():
            if lang in http_langs and http_langs[lang] != href:
                return True

        return False

    def _validate_language_codes(self, all_tags):
        """Validate language codes against standard format.

        Args:
            all_tags (list): List of all hreflang tags.

        Returns:
            list: List of invalid language codes.
        """
        invalid_codes = []

        for tag in all_tags:
            lang = tag["lang"]

            # Skip x-default which is valid
            if lang == "x-default":
                continue

            # Check against standard format: language code (en) or language-country (en-us)
            # More comprehensive validation could be implemented here
            if not re.match(r'^[a-z]{2}(-[a-z]{2,3})?$', lang, re.IGNORECASE):
                invalid_codes.append(lang)

        return invalid_codes

    def _identify_issues(self, all_tags, has_self_reference, has_x_default,
                       has_conflicts, invalid_lang_codes, languages, current_url):
        """Identify issues with hreflang implementation.

        Args:
            all_tags (list): List of all hreflang tags.
            has_self_reference (bool): Whether a self-reference exists.
            has_x_default (bool): Whether an x-default tag exists.
            has_conflicts (bool): Whether conflicts exist.
            invalid_lang_codes (list): List of invalid language codes.
            languages (list): List of all language codes.
            current_url (str): Current page URL.

        Returns:
            list: List of identified issues.
        """
        issues = []

        # No hreflang tags
        if not all_tags:
            issues.append("no_hreflang_tags")
            return issues  # No need to check further

        # Missing self-reference
        if not has_self_reference and current_url:
            issues.append("missing_self_reference")

        # Missing x-default (only an issue when multiple languages exist)
        if not has_x_default and len(set(languages)) > 1:
            issues.append("missing_x_default")

        # Conflicts between HTML and HTTP implementations
        if has_conflicts:
            issues.append("html_http_conflicts")

        # Invalid language codes
        if invalid_lang_codes:
            issues.append("invalid_language_codes")

        # Check for non-absolute URLs
        non_absolute_urls = [tag["href"] for tag in all_tags
                            if not tag["href"].startswith(("http://", "https://"))]
        if non_absolute_urls:
            issues.append("non_absolute_urls")

        # Duplicate language codes
        if len(languages) != len(set(languages)):
            issues.append("duplicate_language_codes")

        return issues


def main():
    """Main entry point for the Hreflang Parser Worker."""
    parser = argparse.ArgumentParser(description="Hreflang Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = HreflangWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()