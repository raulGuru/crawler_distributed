"""Links Parser Worker module.

This module contains the LinksWorker class which extracts comprehensive hyperlink
information from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import argparse
from urllib.parse import urlparse, urljoin

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class LinksWorker(BaseParserWorker):
    """Worker for extracting comprehensive hyperlink information from HTML files.

    This worker processes HTML files saved by the crawler, extracts all link-related data
    including URLs, anchor text, attributes, categorizes links by type (internal/external,
    mailto, tel, javascript, etc.), analyzes link quality metrics, and identifies potential
    issues. The results are stored in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the LinksWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_links_tube",
            task_type="links",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "links_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract comprehensive links data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted links data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)
            base_url = self._extract_base_url(soup, html_path)

            # Initialize containers for all link types
            all_links = []
            internal_links = []
            external_links = []
            nofollow_links = []
            sponsored_links = []
            ugc_links = []
            javascript_links = []
            fragment_links = []
            mailto_links = []
            tel_links = []
            missing_link_text = []

            # Track unique URLs and domains
            unique_internal_urls = set()
            unique_external_urls = set()
            external_domains = set()

            # Track anchor text frequency for internal links
            internal_anchor_texts = {}

            # Process each anchor element
            for anchor in soup.find_all("a", href=True):
                link_data = self._process_link(anchor, base_url)

                if link_data:
                    all_links.append(link_data)

                    # Categorize links by type
                    if link_data["is_internal"]:
                        internal_links.append(link_data)
                        unique_internal_urls.add(link_data["url"])

                        # Track internal anchor text frequency
                        if link_data["text"]:
                            text_lower = link_data["text"].lower()
                            internal_anchor_texts[text_lower] = internal_anchor_texts.get(text_lower, 0) + 1

                    if link_data["is_external"]:
                        external_links.append(link_data)
                        unique_external_urls.add(link_data["url"])
                        if link_data["domain"]:
                            external_domains.add(link_data["domain"])

                    if link_data["is_nofollow"]:
                        nofollow_links.append(link_data)

                    if link_data["is_sponsored"]:
                        sponsored_links.append(link_data)

                    if link_data["is_ugc"]:
                        ugc_links.append(link_data)

                    if link_data["is_javascript"]:
                        javascript_links.append(link_data)

                    if link_data["is_fragment"]:
                        fragment_links.append(link_data)

                    if link_data["is_mailto"]:
                        mailto_links.append(link_data)

                    if link_data["is_tel"]:
                        tel_links.append(link_data)

                    if not link_data["text"] and not link_data["is_fragment"]:
                        missing_link_text.append(link_data)

            # Look for image, CSS, and JS links
            image_links = self._extract_resource_links(soup, "img", "src", "image", base_url)
            css_links = self._extract_resource_links(soup, "link[rel='stylesheet']", "href", "css", base_url)
            js_links = self._extract_resource_links(soup, "script[src]", "src", "js", base_url)

            # Check for link issues
            issues = self._analyze_link_issues(
                all_links,
                missing_link_text,
                nofollow_links,
                external_links
            )

            # Compile the final data structure
            links_data = {
                "all_links": all_links,
                "total_links": len(all_links),
                "internal_links": internal_links,
                "internal_link_count": len(internal_links),
                "unique_internal_count": len(unique_internal_urls),
                "external_links": external_links,
                "external_link_count": len(external_links),
                "unique_external_count": len(unique_external_urls),
                "unique_external_domains": len(external_domains),
                "nofollow_links": nofollow_links,
                "nofollow_count": len(nofollow_links),
                "sponsored_links": sponsored_links,
                "sponsored_count": len(sponsored_links),
                "ugc_links": ugc_links,
                "ugc_count": len(ugc_links),
                "javascript_links": javascript_links,
                "javascript_count": len(javascript_links),
                "fragment_links": fragment_links,
                "fragment_count": len(fragment_links),
                "mailto_links": mailto_links,
                "mailto_count": len(mailto_links),
                "tel_links": tel_links,
                "tel_count": len(tel_links),
                "missing_link_text": missing_link_text,
                "missing_link_text_count": len(missing_link_text),
                "image_links": image_links,
                "image_link_count": len(image_links),
                "css_links": css_links,
                "css_link_count": len(css_links),
                "js_links": js_links,
                "js_link_count": len(js_links),
                "internal_anchor_texts": internal_anchor_texts,
                "issues": issues
            }

            self.logger.debug(
                f"Extracted {len(all_links)} links ({len(internal_links)} internal, "
                f"{len(external_links)} external) for doc_id: {doc_id_str}"
            )

            return links_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_base_url(self, soup, html_path):
        """Extract the base URL from HTML or use the filename as fallback.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_path (str): Path to the HTML file.

        Returns:
            str: Base URL for resolving relative links.
        """
        # Try to get base URL from canonical link
        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get("href"):
            canonical_url = canonical_tag.get("href")
            return self._get_base_domain_url(canonical_url)

        # Try to get from Open Graph URL
        og_url = soup.find("meta", property="og:url")
        if og_url and og_url.get("content"):
            og_url_content = og_url.get("content")
            return self._get_base_domain_url(og_url_content)

        # Use base tag if present
        base_tag = soup.find("base", href=True)
        if base_tag:
            return base_tag["href"]

        # Fallback to extracting from the file path
        file_name = os.path.basename(html_path)
        if file_name.startswith(("http", "www")):
            parts = file_name.split('_')
            if len(parts) >= 3:
                protocol = parts[0].replace('https', 'https://').replace('http', 'http://')
                domain_parts = []
                for part in parts[1:]:
                    if part.endswith('.html'):
                        part = part.split('.')[0]  # Remove .html extension
                    if '.' in part:  # This might be the TLD
                        domain_parts.append(part)
                        break
                    domain_parts.append(part)

                return f"{protocol}{'.'.join(domain_parts)}"

        # If all else fails, use placeholder domain
        self.logger.warning(f"Could not determine base URL for {html_path}, using placeholder")
        return "https://example.com"

    def _get_base_domain_url(self, url):
        """Extract the base domain URL (scheme + netloc) from a full URL.

        Args:
            url (str): Full URL.

        Returns:
            str: Base domain URL.
        """
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _process_link(self, anchor, base_url):
        """Process a single anchor element.

        Args:
            anchor (Tag): BeautifulSoup Tag object representing an anchor.
            base_url (str): Base URL for resolving relative links.

        Returns:
            dict: Processed link data or None if link should be skipped.
        """
        href = anchor.get("href", "").strip()

        # Skip empty hrefs
        if not href:
            return None

        # Get link attributes
        rel = anchor.get("rel", [])
        rel_str = " ".join(rel) if rel else None
        target = anchor.get("target")
        title = anchor.get("title")

        # Extract link text
        link_text = self._clean_text(anchor.get_text())

        # Determine link type
        is_javascript = href.startswith('javascript:')
        is_mailto = href.startswith('mailto:')
        is_tel = href.startswith('tel:')
        is_fragment = href.startswith('#') or (urlparse(href).fragment and not urlparse(href).path)

        # Convert relative URLs to absolute
        if not (is_javascript or is_mailto or is_tel):
            if not href.startswith(('http://', 'https://')):
                href = urljoin(base_url, href)

        # Extract domain and determine if internal/external
        domain = None
        is_internal = False
        is_external = False

        if is_javascript or is_mailto or is_tel or is_fragment:
            # These are special link types
            is_internal = True  # Consider these internal by default
        else:
            # Regular HTTP links
            try:
                parsed_url = urlparse(href)
                domain = parsed_url.netloc

                # Compare domains to determine if internal
                base_domain = urlparse(base_url).netloc

                # Consider www and non-www versions the same
                base_domain_parts = base_domain.split('.')
                if base_domain_parts[0] == 'www' and len(base_domain_parts) > 2:
                    base_domain_no_www = '.'.join(base_domain_parts[1:])
                else:
                    base_domain_no_www = base_domain

                # Check if domains match or if link domain is subdomain of base
                is_internal = (
                    domain == base_domain or
                    domain == base_domain_no_www or
                    (domain.endswith(f".{base_domain_no_www}") and domain != base_domain_no_www)
                )
                is_external = not is_internal and domain != ""
            except Exception as e:
                self.logger.warning(f"Error parsing URL {href}: {e}")
                # If we can't parse it, consider it external
                is_external = True

        # Check for rel attributes
        is_nofollow = 'nofollow' in rel if rel else False
        is_sponsored = 'sponsored' in rel if rel else False
        is_ugc = 'ugc' in rel if rel else False
        opens_new_tab = target == '_blank'

        # Create the link object
        link_data = {
            "url": href,
            "text": link_text,
            "domain": domain,
            "rel": rel_str,
            "target": target,
            "title": title,
            "is_internal": is_internal,
            "is_external": is_external,
            "is_javascript": is_javascript,
            "is_mailto": is_mailto,
            "is_tel": is_tel,
            "is_fragment": is_fragment,
            "is_nofollow": is_nofollow,
            "is_sponsored": is_sponsored,
            "is_ugc": is_ugc,
            "opens_new_tab": opens_new_tab
        }

        return link_data

    def _extract_resource_links(self, soup, selector, attr, link_type, base_url):
        """Extract links from resource elements like images, CSS, and scripts.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            selector (str): CSS selector for the resource elements.
            attr (str): Attribute containing the URL.
            link_type (str): Type of resource ("image", "css", "js").
            base_url (str): Base URL for resolving relative URLs.

        Returns:
            list: List of resource link objects.
        """
        links = []
        for element in soup.select(selector):
            url = element.get(attr)
            if url:
                # Convert relative URLs to absolute
                if not url.startswith(('http://', 'https://')):
                    url = urljoin(base_url, url)

                links.append({
                    "url": url,
                    "type": link_type
                })

        return links

    def _analyze_link_issues(self, all_links, missing_link_text, nofollow_links, external_links):
        """Analyze links for potential issues.

        Args:
            all_links (list): All link objects.
            missing_link_text (list): Links missing anchor text.
            nofollow_links (list): Links with nofollow attribute.
            external_links (list): External links.

        Returns:
            list: Identified issues.
        """
        issues = []

        # Check for excessive links
        if len(all_links) > 100:
            issues.append("excessive_links")

        # Check for missing link text
        if len(missing_link_text) > 0:
            issues.append("missing_link_text")

        # Check if more than 50% of links are nofollow
        if len(nofollow_links) > len(all_links) / 2 and len(all_links) > 0:
            issues.append("excessive_nofollow")

        # Check for unattributed external links
        unattributed_external = [
            link for link in external_links
            if not link["is_nofollow"] and not link["is_sponsored"] and not link["is_ugc"]
        ]
        if unattributed_external:
            issues.append("unattributed_external_links")

        return issues

    def _clean_text(self, text):
        """Clean and normalize text content.

        Args:
            text (str): Raw text to clean.

        Returns:
            str: Cleaned text.
        """
        if not text:
            return None

        # Strip whitespace and normalize spaces
        cleaned = ' '.join(text.split())
        return cleaned


def main():
    """Main entry point for the Links Parser Worker."""
    parser = argparse.ArgumentParser(description="Links Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = LinksWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()