import re
import logging
from typing import Dict, Any, Callable, List, Union, Optional, Tuple
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class BaseExtractor:
    """
    Base extraction framework to standardize and simplify data extraction from web pages.

    This class provides common extraction utilities and patterns that can be reused
    across different extraction tasks in the SEO crawler.
    """

    def extract_with_selectors(self, html_content, selectors: Dict[str, str],
                          processors: Optional[Dict[str, Callable]] = None) -> Dict[str, Any]:
        """
        Extract data using CSS or XPath selectors with optional post-processing.

        Args:
            html_content: Raw HTML content as a string
            selectors: Dict mapping field names to CSS/XPath selectors
            processors: Optional dict mapping field names to processor functions

        Returns:
            Dict containing the extracted data
        """
        soup = BeautifulSoup(html_content, "lxml")
        result = {}

        for field, selector in selectors.items():
            try:
                # Determine if selector is CSS or XPath
                if selector.startswith('//'):
                    # For XPath, we need to use lxml's methods
                    elements = soup.find('html').xpath(selector)
                    value = elements[0] if elements else None
                else:
                    # For CSS selectors, use select_one()
                    element = soup.select_one(selector)
                    value = element.get_text(strip=True) if element else None

                # Apply processor if available
                if processors and field in processors and value is not None:
                    value = processors[field](value)

                result[field] = value
            except Exception as e:
                logger.error(f"Error extracting {field} with selector {selector}: {e}")
                result[field] = None

        return result


    def extract_multiple(self, response, selector: str, is_xpath: bool = False) -> List[str]:
        """
        Extract multiple elements matching a selector.

        Args:
            response: Scrapy response object
            selector: CSS or XPath selector
            is_xpath: Whether the selector is XPath (default: False, meaning CSS)

        Returns:
            List of extracted strings
        """
        try:
            if is_xpath:
                elements = response.xpath(selector).getall()
            else:
                elements = response.css(selector).getall()

            return elements
        except Exception as e:
            logger.error(f"Error extracting multiple elements with selector {selector}: {e}")
            return []

    def extract_structured_list(self, response, item_selector: str,
                               field_selectors: Dict[str, str],
                               is_xpath: bool = False) -> List[Dict[str, Any]]:
        """
        Extract a list of structured items.

        Args:
            response: Scrapy response object
            item_selector: Selector to find each item
            field_selectors: Dict of field name to relative selector for each field
            is_xpath: Whether selectors are XPath (default: False)

        Returns:
            List of dictionaries, each containing extracted fields for one item
        """
        result = []

        try:
            if is_xpath:
                items = response.xpath(item_selector)
            else:
                items = response.css(item_selector)

            for item in items:
                item_data = {}
                for field, selector in field_selectors.items():
                    if is_xpath:
                        value = item.xpath(selector).get()
                    else:
                        value = item.css(selector).get()

                    item_data[field] = value

                result.append(item_data)

            return result
        except Exception as e:
            logger.error(f"Error extracting structured list with selector {item_selector}: {e}")
            return []

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """
        Clean extracted text by removing extra whitespace and normalizing content.

        Args:
            text: Text to clean

        Returns:
            Cleaned text
        """
        if text is None:
            return None

        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', text.strip())
        return cleaned

    def get_text_metrics(self, text: Optional[str]) -> Dict[str, Any]:
        """
        Extract common text metrics (length, word count, etc.)

        Args:
            text: Text to analyze

        Returns:
            Dict with text metrics
        """
        if not text:
            return {
                'length': 0,
                'word_count': 0,
                'has_text': False
            }

        cleaned_text = self.clean_text(text)
        words = cleaned_text.split() if cleaned_text else []

        return {
            'length': len(cleaned_text) if cleaned_text else 0,
            'word_count': len(words),
            'has_text': len(cleaned_text) > 0 if cleaned_text else False
        }

    def extract_from_html(self, html_content: str, selector: str, is_xpath: bool = False) -> Optional[str]:
        """
        Extract content from raw HTML string using BeautifulSoup.

        Args:
            html_content: Raw HTML content
            selector: CSS selector
            is_xpath: Not used, BeautifulSoup doesn't support XPath well

        Returns:
            Extracted content or None
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            if is_xpath:
                logger.warning("XPath not fully supported in BeautifulSoup - using CSS selector fallback")

            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
            return None
        except Exception as e:
            logger.error(f"Error extracting from HTML with selector {selector}: {e}")
            return None

    def get_url_components(self, url: str) -> Dict[str, Any]:
        """
        Extract components from a URL.

        Args:
            url: URL to parse

        Returns:
            Dict with URL components
        """
        try:
            parsed = urlparse(url)
            return {
                'scheme': parsed.scheme,
                'netloc': parsed.netloc,
                'path': parsed.path,
                'params': parsed.params,
                'query': parsed.query,
                'fragment': parsed.fragment,
                'domain': parsed.netloc.split(':')[0]
            }
        except Exception as e:
            logger.error(f"Error parsing URL {url}: {e}")
            return {}

    def with_error_handling(self, extraction_func: Callable, default_value: Any = None) -> Any:
        """
        Decorator pattern for adding error handling to an extraction function.

        Args:
            extraction_func: Function to wrap with error handling
            default_value: Value to return if extraction fails

        Returns:
            Wrapped function result or default value on error
        """
        try:
            return extraction_func()
        except Exception as e:
            logger.error(f"Extraction error in {extraction_func.__name__}: {e}")
            return default_value