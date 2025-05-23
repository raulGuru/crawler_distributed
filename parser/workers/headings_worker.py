"""Headings Parser Worker module.

This module contains the HeadingsWorker class which extracts heading elements
and their structural relationships from saved HTML files as part of a
distributed crawl-parser system.
"""

import os
import sys
import argparse
import re
from difflib import SequenceMatcher
from typing import List, Dict, Set

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class HeadingsWorker(BaseParserWorker):
    """Worker for extracting heading elements and structure from HTML files.

    This worker processes HTML files saved by the crawler, extracts all heading
    elements (H1-H6), analyzes their structure, content, and relationships,
    and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the HeadingsWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_headings_tube",
            task_type="headings",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "headings_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract headings data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted headings data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Get the page title for comparison
            title_tag = soup.title
            page_title = title_tag.string.strip() if title_tag and title_tag.string else ""

            # Extract heading data for H1-H6
            heading_levels = {}
            all_headings = []

            # Process each heading level
            for level in range(1, 7):
                h_tags = soup.find_all(f'h{level}')
                heading_data = self._process_heading_level(h_tags, level)
                heading_levels[f'h{level}'] = heading_data

                # Store all headings in order of appearance for structure analysis
                for h_tag in h_tags:
                    text = self._get_heading_text(h_tag)
                    if text:
                        all_headings.append({
                            'level': level,
                            'text': text
                        })

            # Analyze heading structure
            structure_analysis = self._analyze_heading_structure(all_headings)

            # Calculate similarity between page title and H1
            page_title_similarity = None
            if page_title and heading_levels['h1']['elements']:
                first_h1 = heading_levels['h1']['elements'][0]['text']
                page_title_similarity = self._calculate_similarity(page_title, first_h1)

            # Calculate keyword consistency
            keyword_consistency = self._analyze_keyword_consistency(
                page_title,
                heading_levels['h1']['elements'],
                heading_levels['h2']['elements']
            )

            # Collect overall issues with specific check for keyword consistency
            overall_issues = self._collect_overall_issues(heading_levels, structure_analysis)
            if keyword_consistency.get('title_h1_overlap_percent', 0) < 10:
                overall_issues.append('low_title_h1_keyword_consistency')
            if keyword_consistency.get('h1_h2_overlap_percent', 0) < 10:
                overall_issues.append('low_h1_h2_keyword_consistency')

            # Construct the final data structure
            headings_data = {
                'h1': heading_levels['h1'],
                'h2': heading_levels['h2'],
                'h3': heading_levels['h3'],
                'h4': heading_levels['h4'],
                'h5': heading_levels['h5'],
                'h6': heading_levels['h6'],
                'page_title_similarity': page_title_similarity,
                'keyword_consistency': keyword_consistency,
                'heading_structure': structure_analysis,
                'overall_issues': overall_issues
            }

            self.logger.debug(
                f"Extracted headings_data for doc_id: {doc_id_str}"
            )

            return headings_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _get_heading_text(self, heading_tag) -> str:
        """Extract and clean text from a heading tag.

        Args:
            heading_tag: The BeautifulSoup tag object.

        Returns:
            str: The cleaned text content.
        """
        # Get all text nodes, joining with spaces
        if heading_tag.string:
            # Simple case - heading has just a text node
            return heading_tag.string.strip()
        else:
            # Heading might contain other elements
            return ' '.join([text.strip() for text in heading_tag.stripped_strings])

    def _has_html_elements(self, heading_tag) -> bool:
        """Check if a heading tag contains HTML elements.

        Args:
            heading_tag: The BeautifulSoup tag object.

        Returns:
            bool: True if the heading contains HTML elements.
        """
        return len(heading_tag.find_all()) > 0

    def _process_heading_level(self, headings, level: int) -> dict:
        """Process a set of headings of a specific level.

        Args:
            headings: List of BeautifulSoup heading tags.
            level: The heading level (1-6).

        Returns:
            dict: Structured data about the heading level.
        """
        heading_count = len(headings)
        elements = []
        lengths = []
        empty_count = 0
        seen_texts = set()
        duplicate_texts = set()
        issues = []

        for h_tag in headings:
            text = self._get_heading_text(h_tag)

            if not text:
                empty_count += 1
                continue

            length = len(text)
            lengths.append(length)

            element = {
                'text': text,
                'length': length,
                'contains_html': self._has_html_elements(h_tag)
            }
            elements.append(element)

            # Check for duplicate text
            if text in seen_texts:
                duplicate_texts.add(text)
            else:
                seen_texts.add(text)

        # Calculate length statistics
        min_length = min(lengths) if lengths else 0
        max_length = max(lengths) if lengths else 0
        avg_length = sum(lengths) / len(lengths) if lengths else 0

        # Check for issues
        if level == 1 and heading_count == 0:
            issues.append('missing_h1')
        elif level == 1 and heading_count > 1:
            issues.append('multiple_h1s')

        if empty_count > 0:
            issues.append(f'empty_h{level}_elements')

        if duplicate_texts:
            issues.append(f'duplicate_h{level}_text')

        # Assemble the data structure
        level_data = {
            'count': heading_count,
            'elements': elements,
            f'has_h{level}': heading_count > 0,
            f'has_multiple_h{level}s': heading_count > 1,
            'length': {
                'min': min_length,
                'max': max_length,
                'avg': round(avg_length, 1)  # Round to 1 decimal place
            },
            'empty_elements': empty_count,
            'duplicate_text': len(duplicate_texts) > 0,
            'issues': issues
        }

        # Only add duplicate_texts if they exist
        if duplicate_texts:
            level_data['duplicate_texts'] = list(duplicate_texts)

        return level_data

    def _analyze_heading_structure(self, headings: List[Dict]) -> dict:
        """Analyze the overall structure of headings.

        Args:
            headings: List of heading dictionaries with 'level' and 'text'.

        Returns:
            dict: Analysis of heading structure.
        """
        proper_hierarchy = True
        missing_levels = []
        nesting_issues = []
        structure_issues = []

        # Check if first heading is H1
        if headings and headings[0]['level'] != 1:
            proper_hierarchy = False
            structure_issues.append('first_heading_not_h1')

        # Check for skipped levels
        prev_level = 0
        for heading in headings:
            current_level = heading['level']

            # Skip the initial check since we already checked if first is H1
            if prev_level > 0:
                # If we jump more than one level (e.g., H1 to H3, skipping H2)
                if current_level > prev_level + 1:
                    proper_hierarchy = False
                    missing_level = prev_level + 1
                    missing_levels.append(missing_level)
                    structure_issues.append(f'skipped_h{missing_level}')

                # If we jump backward multiple levels (e.g., H4 to H2)
                # This isn't necessarily an error but worth noting
                if current_level < prev_level - 1:
                    nesting_issues.append(f'backward_jump_h{prev_level}_to_h{current_level}')

            prev_level = current_level

        return {
            'proper_hierarchy': proper_hierarchy,
            'missing_levels': missing_levels,
            'nesting_issues': nesting_issues,
            'issues': structure_issues
        }

    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings.

        Args:
            str1: First string.
            str2: Second string.

        Returns:
            float: Similarity score between 0 and 1.
        """
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

    def _extract_keywords(self, text: str) -> Set[str]:
        """Extract meaningful keywords from text.

        Args:
            text: The text to extract keywords from.

        Returns:
            set: Set of keywords.
        """
        if not text:
            return set()

        # Convert to lowercase and remove punctuation
        text = re.sub(r'[^\w\s]', ' ', text.lower())

        # Split into words and filter out short words
        words = [word for word in text.split() if len(word) > 3]

        # Remove common stop words
        stop_words = {'this', 'that', 'these', 'those', 'with', 'from', 'their', 'about', 'would', 'could'}
        return {word for word in words if word not in stop_words}

    def _analyze_keyword_consistency(self, title: str, h1_elements: List[Dict], h2_elements: List[Dict]) -> dict:
        """Analyze keyword consistency between title, H1s, and H2s.

        Args:
            title: Page title.
            h1_elements: List of H1 element dictionaries.
            h2_elements: List of H2 element dictionaries.

        Returns:
            dict: Keyword consistency analysis.
        """
        # Extract keywords from title
        title_keywords = self._extract_keywords(title)
        if not title_keywords:
            return {
                'title_h1_overlap': 0,
                'title_h1_overlap_percent': 0.0,
                'title_h2_overlap': 0,
                'title_h2_overlap_percent': 0.0,
                'h1_h2_overlap': 0,
                'h1_h2_overlap_percent': 0.0
            }

        # Extract keywords from H1s and H2s
        h1_keywords = set()
        for h1 in h1_elements:
            h1_keywords.update(self._extract_keywords(h1['text']))

        h2_keywords = set()
        for h2 in h2_elements:
            h2_keywords.update(self._extract_keywords(h2['text']))

        # Calculate overlaps
        title_h1_overlap = len(title_keywords.intersection(h1_keywords))
        title_h2_overlap = len(title_keywords.intersection(h2_keywords))
        h1_h2_overlap = len(h1_keywords.intersection(h2_keywords))

        # Calculate percentages
        title_h1_percent = round((title_h1_overlap / len(title_keywords)) * 100, 1) if title_keywords else 0.0
        title_h2_percent = round((title_h2_overlap / len(title_keywords)) * 100, 1) if title_keywords else 0.0
        h1_h2_percent = round((h1_h2_overlap / len(h1_keywords)) * 100, 1) if h1_keywords else 0.0

        return {
            'title_h1_overlap': title_h1_overlap,
            'title_h1_overlap_percent': title_h1_percent,
            'title_h2_overlap': title_h2_overlap,
            'title_h2_overlap_percent': title_h2_percent,
            'h1_h2_overlap': h1_h2_overlap,
            'h1_h2_overlap_percent': h1_h2_percent
        }

    def _collect_overall_issues(self, heading_levels: Dict, structure_analysis: Dict) -> List[str]:
        """Collect all issues into a single list.

        Args:
            heading_levels: Dictionary of heading level data.
            structure_analysis: Heading structure analysis.

        Returns:
            list: List of all issues.
        """
        overall_issues = []

        # Add issues from each heading level
        for level in range(1, 7):
            level_key = f'h{level}'
            if level_key in heading_levels and 'issues' in heading_levels[level_key]:
                overall_issues.extend(heading_levels[level_key]['issues'])

        # Add structure issues
        if 'issues' in structure_analysis:
            overall_issues.extend(structure_analysis['issues'])

        # Add any additional checks
        if heading_levels.get('h1', {}).get('count', 0) == 0:
            overall_issues.append('missing_h1')

        # Remove duplicates while preserving order
        seen = set()
        unique_issues = []
        for issue in overall_issues:
            if issue not in seen:
                seen.add(issue)
                unique_issues.append(issue)

        return unique_issues


def main():
    """Main entry point for the Headings Parser Worker."""
    parser = argparse.ArgumentParser(description="Headings Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = HeadingsWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()