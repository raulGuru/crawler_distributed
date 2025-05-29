"""Images Data Parser Worker module.

This module contains the ImagesDataWorker class which extracts image-related data
from saved HTML files as part of a distributed crawl-parser system. It analyzes
all image elements on a page, extracting their attributes and organizing them
into useful categories for SEO analysis.
"""

import os
import sys
import re
import argparse
from urllib.parse import urlparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class ImagesWorker(BaseParserWorker):
    """Worker for extracting image-related data from HTML files.

    This worker processes HTML files saved by the crawler, extracts all image elements
    on the page, analyzes their attributes (URL, alt text, dimensions, etc.), and
    categorizes them for comprehensive SEO analysis.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the ImagesWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_images_tube",
            task_type="images",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "images_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract comprehensive image data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Comprehensive image analysis data organized by categories.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Get the URL from the canonical link or meta property
            page_url = self._extract_page_url(soup)
            if not page_url:
                # Use the filename as a fallback for testing environments
                page_url = os.path.basename(html_path)
                self.logger.warning(f"No URL found in HTML, using filename as fallback: {page_url}")

            # Extract all images from the page
            images = self._extract_all_images(soup, page_url)

            # Create comprehensive analysis structure
            image_analysis = self._create_image_analysis(images)

            return image_analysis

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_page_url(self, soup):
        """Extract the page URL from various possible locations in the HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            str: The page URL or None if not found.
        """
        # Try canonical link first
        canonical = soup.find("link", rel="canonical")
        if canonical and canonical.get("href"):
            return canonical.get("href")

        # Try Open Graph URL
        og_url = soup.find("meta", property="og:url")
        if og_url and og_url.get("content"):
            return og_url.get("content")

        # Try Twitter URL
        twitter_url = soup.find("meta", attrs={"name": "twitter:url"})
        if twitter_url and twitter_url.get("content"):
            return twitter_url.get("content")

        return None

    def _extract_all_images(self, soup, page_url):
        """Extract and analyze all images on the page.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            page_url (str): URL of the page being processed.

        Returns:
            list: List of image objects with their attributes.
        """
        images = []

        # Parse the page URL to get domain for internal/external checking
        try:
            page_domain = urlparse(page_url).netloc
        except Exception:
            # If parsing fails, use a fallback domain
            page_domain = ''
            self.logger.warning(f"Failed to parse domain from URL: {page_url}")

        # Find all img tags in the document
        img_tags = soup.find_all("img")

        for img in img_tags:
            # Get src attribute or skip if not present
            src = img.get("src")
            if not src:
                # Check for data-src as a fallback (common in lazy-loaded images)
                src = img.get("data-src") or img.get("data-original") or img.get("data-lazy-src")
                if not src:
                    continue

            try:
                # Convert relative URLs to absolute
                if not src.startswith(('http://', 'https://', 'data:')):
                    # Handle protocol-relative URLs
                    if src.startswith('//'):
                        src = 'https:' + src
                    else:
                        # Join relative URL with page URL
                        base_url = page_url
                        # Remove fragment and query from base URL
                        base_parts = urlparse(base_url)
                        base_url = f"{base_parts.scheme}://{base_parts.netloc}{base_parts.path}"
                        # Normalize path join
                        if not base_url.endswith('/') and not src.startswith('/'):
                            base_url += '/'
                        src = base_url + (src[1:] if src.startswith('/') else src)

                # Extract image attributes
                alt_text = img.get("alt", "")
                title = img.get("title", "")

                # Extract width and height from attributes
                width = img.get("width")
                height = img.get("height")

                # Convert dimensions to integers if possible
                try:
                    width = int(width) if width else None
                    height = int(height) if height else None
                except ValueError:
                    width = None
                    height = None

                # Get image size from style attribute (fallback)
                style = img.get("style", "")
                if (not width or not height) and style:
                    # Try to extract dimensions from style
                    width_match = re.search(r'width:\s*(\d+)px', style)
                    height_match = re.search(r'height:\s*(\d+)px', style)
                    width = int(width_match.group(1)) if width_match and not width else width
                    height = int(height_match.group(1)) if height_match and not height else height

                # Extract image file information from URL
                parsed_url = urlparse(src)
                path = parsed_url.path
                filename = os.path.basename(path) if path else ""
                extension = os.path.splitext(filename)[1].lower() if filename else ""

                # Determine if image is from the same domain
                img_domain = parsed_url.netloc
                is_internal = img_domain == page_domain or not img_domain

                # Create the image object
                image_data = {
                    'url': src,
                    'alt_text': alt_text,
                    'title': title,
                    'width': width,
                    'height': height,
                    'filename': filename,
                    'extension': extension,
                    'is_internal': is_internal
                }

                images.append(image_data)

            except Exception as e:
                self.logger.warning(f"Error processing image {src}: {e}")
                continue

        return images

    def _create_image_analysis(self, images):
        """Create a comprehensive analysis structure from the list of images.

        Args:
            images (list): List of image objects.

        Returns:
            dict: Comprehensive image analysis data organized by categories.
        """
        # Organize results into categories
        image_analysis = {
            'all_images': images,
            'total_images': len(images),
            'missing_alt_text': [img for img in images if not img['alt_text'].strip()],
            'have_alt_text': [img for img in images if img['alt_text'].strip()],
            'missing_title': [img for img in images if not img['title'].strip()],
            'have_title': [img for img in images if img['title'].strip()],
            'oversized_images': [img for img in images if img['width'] and img['height'] and
                            (img['width'] > 1000 or img['height'] > 1000)],
            'undersized_images': [img for img in images if img['width'] and img['height'] and
                            (img['width'] < 100 or img['height'] < 100)],
            'internal_images': [img for img in images if img['is_internal']],
            'external_images': [img for img in images if not img['is_internal']],
            'images_by_type': {},
            'missing_dimensions': [img for img in images if img['width'] is None or img['height'] is None]
        }

        # Group images by file extension
        for img in images:
            ext = img['extension']
            if ext:
                if ext not in image_analysis['images_by_type']:
                    image_analysis['images_by_type'][ext] = []
                image_analysis['images_by_type'][ext].append(img)

        # Add statistical data
        image_analysis['stats'] = {
            'total_count': len(images),
            # TODO: Will be added later
            # 'alt_text_percent': self._calculate_percentage(len(image_analysis['have_alt_text']), len(images)),
            'alt_text_count': len(image_analysis['have_alt_text']),
            'missing_alt_count': len(image_analysis['missing_alt_text']),
            'missing_dimensions_count': len(image_analysis['missing_dimensions']),
            'oversized_count': len(image_analysis['oversized_images']),
            'undersized_count': len(image_analysis['undersized_images']),
            'internal_count': len(image_analysis['internal_images']),
            'external_count': len(image_analysis['external_images']),
            'extension_counts': {ext: len(imgs) for ext, imgs in image_analysis['images_by_type'].items()}
        }

        return image_analysis

    def _calculate_percentage(self, part, whole):
        """Calculate percentage safely, handling divide-by-zero cases.

        Args:
            part (int): The part value.
            whole (int): The whole value.

        Returns:
            float: The percentage (0-100).
        """
        if whole == 0:
            return 0
        return round((part / whole) * 100, 2)


def main():
    """Main entry point for the Images Data Parser Worker."""
    parser = argparse.ArgumentParser(description="Images Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = ImagesWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()