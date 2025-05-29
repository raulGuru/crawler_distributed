"""Page Title Parser Worker module.

This module contains the PageTitleWorker class which extracts title and meta
information from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import argparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class PageTitleWorker(BaseParserWorker):
    """Worker for extracting page title and meta information from HTML files.

    This worker processes HTML files saved by the crawler, extracts various
    title-related elements and meta information from the page's head section,
    and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the PageTitleWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_page_title_tube",
            task_type="page_title",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "page_title"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract page title specific data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted page title data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract basic title information
            title_tag = soup.title
            title_text = (
                title_tag.string.strip() if title_tag and title_tag.string else None
            )
            title_length = len(title_text) if title_text else 0

            # Extract meta information
            meta_data = self._extract_meta_information(soup)

            # Extract hreflang tags
            hreflang_tags = self._extract_hreflang_tags(soup)

            # Extract Open Graph tags
            og_tags = self._extract_og_tags(soup)

            # Extract Twitter tags
            # TODO: Will be added later
            #twitter_tags = self._extract_twitter_tags(soup)

            page_title_data = {
                "title": title_text,
                "title_length": title_length,
                # "meta_keywords": meta_data["keywords"],   # TODO: Will be added later
                "canonical_url": meta_data["canonical_url"],
                "robots": meta_data["robots"],
                "hreflang_tags": hreflang_tags,
                "og_tags": og_tags,
                # "twitter_tags": twitter_tags  # TODO: Will be added later
            }

            self.logger.debug(
                f"Extracted page_title_data: {page_title_data} for doc_id: {doc_id_str}"
            )

            return page_title_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_meta_information(self, soup):
        """Extract meta information from the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Dictionary containing keywords, canonical_url, and robots info.
        """
        meta_info = {}

        # Extract meta keywords
        # TODO: Will be added later
        # meta_keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        # meta_info["keywords"] = (
        #     meta_keywords_tag.get("content", "").strip()
        #     if meta_keywords_tag else None
        # )

        # Extract canonical URL
        canonical_tag = soup.find("link", rel="canonical")
        meta_info["canonical_url"] = canonical_tag.get("href") if canonical_tag else None

        # Extract robots meta tag
        robots_tag = soup.find("meta", attrs={"name": "robots"})
        meta_info["robots"] = (
            robots_tag.get("content", "").strip()
            if robots_tag else None
        )

        return meta_info

    def _extract_hreflang_tags(self, soup):
        """Extract hreflang tags from the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            list: List of dictionaries containing hreflang data.
        """
        hreflang_tags = []
        for tag in soup.find_all("link", rel="alternate", hreflang=True):
            hreflang_data = {
                "lang": tag.get("hreflang"),
                "href": tag.get("href")
            }
            hreflang_tags.append(hreflang_data)
        return hreflang_tags

    def _extract_og_tags(self, soup):
        """Extract Open Graph tags from the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Dictionary containing Open Graph tag values.
        """
        og_tags = {}

        # Define the OG properties we're interested in
        og_properties = {
            "og:locale": "locale",
            "og:type": "type",
            "og:title": "title",
            "og:description": "description",
            "og:url": "url",
            "og:site_name": "site_name",
            "og:image": "image",
            "og:image:width": "image_width",
            "og:image:height": "image_height",
            "og:image:type": "image_type"
        }

        for og_property, field_name in og_properties.items():
            og_tag = soup.find("meta", property=og_property)
            if og_tag and og_tag.get("content"):
                og_tags[field_name] = og_tag["content"].strip()
            else:
                og_tags[field_name] = None

        return og_tags

    def _extract_twitter_tags(self, soup):
        """Extract Twitter Card tags from the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Dictionary containing Twitter Card tag values.
        """
        twitter_tags = {}

        # Define the Twitter properties we're interested in
        twitter_properties = [
            "card",
            "site",
            "creator",
            "title",
            "description",
            "image",
            "image:alt",
            "player",
            "player:width",
            "player:height",
            "player:stream",
            "app:id:iphone",
            "app:id:ipad",
            "app:id:googleplay"
        ]

        for prop in twitter_properties:
            # Twitter tags can be name="twitter:prop" or property="twitter:prop"
            twitter_tag = (
                soup.find("meta", attrs={"name": f"twitter:{prop}"}) or
                soup.find("meta", property=f"twitter:{prop}")
            )

            if twitter_tag and twitter_tag.get("content"):
                twitter_tags[prop] = twitter_tag["content"].strip()
            else:
                twitter_tags[prop] = None

        return twitter_tags


def main():
    """Main entry point for the Page Title Parser Worker."""
    parser = argparse.ArgumentParser(description="Page Title Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = PageTitleWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()