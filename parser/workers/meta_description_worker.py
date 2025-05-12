"""Meta Description Parser Worker module.

This module contains the MetaDescriptionWorker class which extracts meta description
content from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
from datetime import datetime
import argparse
import time

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class MetaDescriptionWorker(BaseParserWorker):
    """Worker for extracting meta description content from HTML files.

    This worker processes HTML files saved by the crawler, extracts the meta
    description tag content from the page's head section, and stores the
    results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the MetaDescriptionWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="htmlparser_meta_description_extraction_tube",
            task_type="meta_description_extraction",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "meta_description"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str) -> dict:
        """Extract meta description data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Contains the extracted meta description content.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract meta description
            meta_description = self._extract_meta_description(soup)

            # Since the schema only requires the meta description content,
            # we return a simple string value as the data
            self.logger.debug(
                f"Extracted meta_description: {meta_description} for doc_id: {doc_id_str}"
            )

            return meta_description

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_meta_description(self, soup):
        """Extract meta description content from HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            str or None: The meta description content, or None if not found.
        """
        # Look for meta description tag
        meta_desc_tag = soup.find("meta", attrs={"name": "description"})

        if meta_desc_tag and meta_desc_tag.has_attr("content"):
            content = meta_desc_tag.get("content", "").strip()
            # Return None for empty strings to match the nullable requirement
            return content if content else None

        return None


def main():
    """Main entry point for the Meta Description Parser Worker."""
    parser = argparse.ArgumentParser(description="Meta Description Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = MetaDescriptionWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()