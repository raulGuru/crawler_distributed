"""Structured Data Parser Worker module.

This module contains the StructuredWorker class which extracts structured data
information (JSON-LD, Microdata, RDFa) from saved HTML files as part of a
distributed crawl-parser system.
"""

import os
import sys
import json
from urllib.parse import urlparse
import argparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class StructuredWorker(BaseParserWorker):
    """Worker for extracting structured data information from HTML files.

    This worker processes HTML files saved by the crawler, extracts structured data
    in various formats (JSON-LD, Microdata, RDFa), analyzes schema types, and
    stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the StructuredWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_structured_tube",
            task_type="structured",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "structured_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract structured data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted structured data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Get URL from meta tag if available, or derive from file path
            url = self._extract_url_from_metadata(soup, html_path)

            # Initialize structured data container
            structured_data = {
                'json_ld': [],
                'microdata': [],
                'rdfa': [],
                'all_types': [],
                'detected_formats': [],
                'schema_count': 0,
                'has_organization': False,
                'has_website': False,
                'has_webpage': False,
                'has_breadcrumb': False,
                'has_product': False,
                'has_review': False,
                'has_aggregate_rating': False,
                'has_local_business': False,
                'has_article': False,
                'has_event': False,
                'has_recipe': False,
                'has_faq': False,
                'has_person': False,
                'has_video': False,
                'has_how_to': False,
                'issues': []
            }

            # Extract JSON-LD
            self._extract_json_ld(soup, structured_data)

            # Extract Microdata
            self._extract_microdata(soup, structured_data)

            # Extract RDFa
            self._extract_rdfa(soup, structured_data)

            # Calculate total schema count
            structured_data['schema_count'] = (
                len(structured_data['json_ld']) +
                len(structured_data['microdata']) +
                len(structured_data['rdfa'])
            )

            # Analyze for common implementation issues
            self._analyze_structured_data_issues(soup, url, structured_data)

            self.logger.debug(
                f"Extracted structured_data with {structured_data['schema_count']} schemas for doc_id: {doc_id_str}"
            )

            return structured_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_url_from_metadata(self, soup, html_path):
        """Extract URL from metadata or filename.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_path (str): Path to the HTML file.

        Returns:
            str: URL of the page, or derived from file path if not found.
        """
        # Try to get canonical URL
        canonical_link = soup.find('link', rel='canonical')
        if canonical_link and canonical_link.get('href'):
            return canonical_link['href']

        # Try to get Open Graph URL
        og_url = soup.find('meta', property='og:url')
        if og_url and og_url.get('content'):
            return og_url['content']

        # Fallback to filename
        return os.path.basename(html_path)

    def _extract_json_ld(self, soup, structured_data):
        """Extract JSON-LD data from HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            structured_data (dict): Container to update with extracted data.
        """
        json_ld_scripts = soup.find_all('script', type='application/ld+json')

        if json_ld_scripts:
            structured_data['detected_formats'].append('json_ld')

            for script in json_ld_scripts:
                try:
                    if script.string:
                        json_data = json.loads(script.string)

                        # Handle both single objects and arrays of objects
                        if isinstance(json_data, list):
                            for item in json_data:
                                structured_data['json_ld'].append(item)
                                self._process_schema_item(item, structured_data)
                        else:
                            structured_data['json_ld'].append(json_data)
                            self._process_schema_item(json_data, structured_data)

                except (json.JSONDecodeError, ValueError):
                    structured_data['issues'].append('invalid_json_ld')

    def _extract_microdata(self, soup, structured_data):
        """Extract Microdata from HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            structured_data (dict): Container to update with extracted data.
        """
        itemscope_elements = soup.find_all(attrs={'itemscope': True})

        if itemscope_elements:
            structured_data['detected_formats'].append('microdata')

            for element in itemscope_elements:
                item_type = element.get('itemtype')

                if item_type:
                    schema_type = self._extract_schema_type(item_type)
                    microdata_item = {'@type': schema_type}

                    # Add to all_types for analysis
                    structured_data['all_types'].append(schema_type)
                    structured_data['microdata'].append(microdata_item)

                    # Check schema type
                    self._check_schema_type(schema_type, structured_data)

                    # Check for nested itemscope elements
                    nested_items = len(element.find_all(attrs={'itemscope': True}, recursive=False))
                    if nested_items > 0:
                        microdata_item['nested_items'] = nested_items

                    # Extract itemprop elements (basic implementation)
                    item_props = element.find_all(attrs={'itemprop': True})
                    if item_props:
                        props = {}
                        for prop in item_props:
                            prop_name = prop.get('itemprop')
                            if prop_name:
                                if prop.name == 'meta':
                                    props[prop_name] = prop.get('content')
                                elif prop.name in ['img', 'audio', 'embed', 'iframe', 'source', 'track', 'video']:
                                    props[prop_name] = prop.get('src')
                                elif prop.name == 'a' or prop.name == 'link':
                                    props[prop_name] = prop.get('href')
                                elif prop.name == 'time':
                                    props[prop_name] = prop.get('datetime')
                                else:
                                    props[prop_name] = prop.text.strip()

                        if props:
                            microdata_item['properties'] = props

    def _extract_rdfa(self, soup, structured_data):
        """Extract RDFa data from HTML.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            structured_data (dict): Container to update with extracted data.
        """
        rdfa_elements = soup.find_all(attrs={'typeof': True})

        if rdfa_elements:
            structured_data['detected_formats'].append('rdfa')

            for element in rdfa_elements:
                rdfa_type = element.get('typeof')

                if rdfa_type:
                    # Extract schema type from RDFa type
                    schema_type = rdfa_type.split(':')[-1] if ':' in rdfa_type else rdfa_type

                    rdfa_item = {'@type': schema_type}
                    structured_data['all_types'].append(schema_type)
                    structured_data['rdfa'].append(rdfa_item)

                    # Check schema type
                    self._check_schema_type(schema_type, structured_data)

                    # Extract basic properties (property attribute)
                    properties = {}
                    prop_elements = element.find_all(attrs={'property': True})

                    for prop in prop_elements:
                        prop_name = prop.get('property').split(':')[-1] if ':' in prop.get('property') else prop.get('property')

                        # Extract appropriate value based on element type
                        if prop.name == 'meta':
                            prop_value = prop.get('content')
                        elif prop.name == 'img':
                            prop_value = prop.get('src')
                        elif prop.name == 'a':
                            prop_value = prop.get('href')
                        else:
                            prop_value = prop.text.strip()

                        properties[prop_name] = prop_value

                    if properties:
                        rdfa_item['properties'] = properties

    def _process_schema_item(self, item, structured_data):
        """Process a schema item and update the structured_data container.

        Args:
            item (dict): Schema item to process.
            structured_data (dict): Container to update with extracted data.
        """
        # Extract schema type
        schema_type = item.get('@type')

        # Handle array of types
        if isinstance(schema_type, list):
            for type_item in schema_type:
                structured_data['all_types'].append(type_item)
                self._check_schema_type(type_item, structured_data)
        elif schema_type:
            structured_data['all_types'].append(schema_type)
            self._check_schema_type(schema_type, structured_data)

        # Check for nested types in properties
        for key, value in item.items():
            if isinstance(value, dict) and '@type' in value:
                nested_type = value.get('@type')
                if nested_type:
                    structured_data['all_types'].append(nested_type)
                    self._check_schema_type(nested_type, structured_data)

            # Handle arrays of objects
            elif isinstance(value, list):
                for list_item in value:
                    if isinstance(list_item, dict) and '@type' in list_item:
                        nested_list_type = list_item.get('@type')
                        if nested_list_type:
                            structured_data['all_types'].append(nested_list_type)
                            self._check_schema_type(nested_list_type, structured_data)

        # Process @graph container which can hold multiple schema objects
        if '@graph' in item:
            for graph_item in item.get('@graph', []):
                if isinstance(graph_item, dict):
                    # Process each item in the graph
                    graph_type = graph_item.get('@type')
                    if graph_type:
                        if isinstance(graph_type, list):
                            for type_item in graph_type:
                                structured_data['all_types'].append(type_item)
                                self._check_schema_type(type_item, structured_data)
                        else:
                            structured_data['all_types'].append(graph_type)
                            self._check_schema_type(graph_type, structured_data)

    def _check_schema_type(self, schema_type, structured_data):
        """Check for specific schema types and update flags in structured_data.

        Args:
            schema_type (str): Schema type to check.
            structured_data (dict): Container to update with flags.
        """
        if not schema_type:
            return

        schema_type_lower = str(schema_type).lower()

        if 'organization' in schema_type_lower:
            structured_data['has_organization'] = True
        elif 'website' in schema_type_lower:
            structured_data['has_website'] = True
        elif 'webpage' in schema_type_lower:
            structured_data['has_webpage'] = True
        elif 'breadcrumb' in schema_type_lower:
            structured_data['has_breadcrumb'] = True
        elif 'product' in schema_type_lower:
            structured_data['has_product'] = True
        elif 'review' in schema_type_lower:
            structured_data['has_review'] = True
        elif 'aggregaterating' in schema_type_lower:
            structured_data['has_aggregate_rating'] = True
        elif 'localbusiness' in schema_type_lower:
            structured_data['has_local_business'] = True
        elif 'article' in schema_type_lower or 'newsarticle' in schema_type_lower or 'blogposting' in schema_type_lower:
            structured_data['has_article'] = True
        elif 'event' in schema_type_lower:
            structured_data['has_event'] = True
        elif 'recipe' in schema_type_lower:
            structured_data['has_recipe'] = True
        elif 'faqpage' in schema_type_lower:
            structured_data['has_faq'] = True
        elif 'person' in schema_type_lower:
            structured_data['has_person'] = True
        elif 'video' in schema_type_lower:
            structured_data['has_video'] = True
        elif 'howto' in schema_type_lower:
            structured_data['has_how_to'] = True

    def _extract_schema_type(self, item_type):
        """Extract schema type from a URL.

        Args:
            item_type (str): Schema.org URL or type string.

        Returns:
            str: The extracted schema type.
        """
        if not item_type:
            return None

        # Extract schema type from URL
        if 'schema.org' in item_type:
            parts = item_type.split('/')
            # Get last non-empty part
            for part in reversed(parts):
                if part:
                    return part

        return item_type

    def _analyze_structured_data_issues(self, soup, url, structured_data):
        """Analyze structured data for common implementation issues.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            url (str): URL of the page being analyzed.
            structured_data (dict): Container to update with issues.
        """
        # Check for multiple formats (not necessarily an issue, but worth noting)
        if len(structured_data['detected_formats']) > 1:
            structured_data['issues'].append('multiple_structured_data_formats')

        # Check for schema.org consistency
        for schema_type in structured_data['all_types']:
            if schema_type and not isinstance(schema_type, str):
                structured_data['issues'].append('non_string_schema_type')
                break

            if schema_type and (schema_type.startswith('http') or '.' in schema_type):
                # Potentially malformed type
                structured_data['issues'].append('possibly_malformed_schema_type')
                break

        # Check for empty schemas
        if structured_data['schema_count'] == 0:
            structured_data['issues'].append('no_structured_data')

        # Check for product pages without product schema
        if 'product' in url.lower() or '/p/' in url.lower():
            product_indicators = [
                soup.find(class_='product-price'),
                soup.find(class_='add-to-cart'),
                soup.find(string=lambda text: text and 'Add to Cart' in text)
            ]

            if any(product_indicators) and not structured_data['has_product']:
                structured_data['issues'].append('missing_product_schema')

        # Check for article pages without article schema
        if 'blog' in url.lower() or 'news' in url.lower() or 'article' in url.lower():
            article_indicators = [
                soup.find('article'),
                soup.find(class_='post'),
                soup.find(class_='entry-date')
            ]

            if any(article_indicators) and not structured_data['has_article']:
                structured_data['issues'].append('missing_article_schema')

        # Check if organizational schema is missing on homepage
        parsed_url = urlparse(url)
        if parsed_url.path in ['/', ''] or url.endswith('/'):
            if not structured_data['has_organization'] and not structured_data['has_website']:
                structured_data['issues'].append('missing_organization_schema_on_homepage')

        # Check for consistency in JSON-LD implementation
        if structured_data['json_ld'] and len(structured_data['json_ld']) > 1:
            # Check for potential duplicate schema types
            types = [item.get('@type') for item in structured_data['json_ld'] if item.get('@type')]
            if len(types) != len(set(types)):
                structured_data['issues'].append('duplicate_schema_types')


def main():
    """Main entry point for the Structured Data Parser Worker."""
    parser = argparse.ArgumentParser(description="Structured Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = StructuredWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()