"""Directives Parser Worker module.

This module contains the DirectivesWorker class which extracts robots directives
from saved HTML files as part of a distributed crawl-parser system.
"""

# TODO: # Extract X-Robots-Tag from metadata

import os
import sys
import re
import argparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class DirectivesWorker(BaseParserWorker):
    """Worker for extracting robots directives from HTML files.

    This worker processes HTML files saved by the crawler, extracts robots directives
    from meta tags, HTTP headers, and analyzes their implications for search engines.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the DirectivesWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_directives_tube",
            task_type="directives",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "directives_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract directives data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted directives data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract meta robots and googlebot tags
            meta_robots_tags = self._extract_meta_tags(soup, "robots")
            meta_googlebot_tags = self._extract_meta_tags(soup, "googlebot")

            # Extract X-Robots-Tag from metadata
            # In a real scenario, this would come from HTTP headers in the response
            # Since we're working with saved HTML files, we'll leave this null
            x_robots_tag = None

            # Get all directives from meta robots and googlebot tags
            all_directives = self._combine_directives(meta_robots_tags, meta_googlebot_tags, x_robots_tag)

            # Check for the presence of specific directives
            directive_flags = self._analyze_directive_presence(all_directives)

            # Count links with specific rel attributes
            link_counts = self._count_rel_attributes(soup)

            # Extract values for directives with parameters
            directive_values = self._extract_directive_values(all_directives)

            # Check for conflicts between directives
            has_conflicts = self._check_directive_conflicts(directive_flags)

            # Analyze issues with directives
            issues = self._analyze_directive_issues(directive_flags, has_conflicts)

            # Determine overall indexability and followability
            indexability = self._determine_indexability(directive_flags)
            followability = self._determine_followability(directive_flags)

            # Construct the final directives data
            directives_data = {
                "meta_robots": meta_robots_tags,
                "meta_googlebot": meta_googlebot_tags,
                "x_robots_tag": x_robots_tag,
                "all_directives": all_directives,
                **directive_flags,
                **link_counts,
                **directive_values,
                "has_conflicts": has_conflicts,
                "issues": issues,
                "is_indexable": indexability,
                "is_followable": followability
            }

            self.logger.debug(
                f"Extracted directives data: {directives_data} for doc_id: {doc_id_str}"
            )

            return directives_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_meta_tags(self, soup, name):
        """Extract content from meta tags with the specified name.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            name (str): Name attribute of the meta tag to extract.

        Returns:
            list: List of directives found in the meta tags.
        """
        directives = []
        meta_tags = soup.find_all("meta", attrs={"name": name})

        for tag in meta_tags:
            content = tag.get("content", "").strip().lower()
            if content:
                # Split by comma and strip whitespace
                tag_directives = [d.strip() for d in content.split(",")]
                directives.extend(tag_directives)

        return directives

    def _combine_directives(self, meta_robots, meta_googlebot, x_robots_tag):
        """Combine directives from different sources.

        Args:
            meta_robots (list): Directives from meta robots tags.
            meta_googlebot (list): Directives from meta googlebot tags.
            x_robots_tag (str): Directives from X-Robots-Tag HTTP header.

        Returns:
            list: Combined list of unique directives.
        """
        all_directives = []
        all_directives.extend(meta_robots)
        all_directives.extend(meta_googlebot)

        if x_robots_tag:
            # Split by comma and strip whitespace
            x_robots_directives = [d.strip() for d in x_robots_tag.split(",")]
            all_directives.extend(x_robots_directives)

        # Remove duplicates while preserving order
        unique_directives = []
        for directive in all_directives:
            if directive not in unique_directives:
                unique_directives.append(directive)

        return unique_directives

    def _analyze_directive_presence(self, all_directives):
        """Check for the presence of specific directives.

        Args:
            all_directives (list): List of all directives.

        Returns:
            dict: Dictionary of directive presence flags.
        """
        # Initialize all flags to False
        directive_flags = {
            "has_noindex": False,
            "has_nofollow": False,
            "has_none": False,
            "has_noarchive": False,
            "has_nosnippet": False,
            "has_notranslate": False,
            "has_noimageindex": False,
            "has_unavailable_after": False,
            "has_max_snippet": False,
            "has_max_image_preview": False,
            "has_max_video_preview": False,
            "has_index": False,
            "has_follow": False,
            "has_all": False
        }

        # Check for each directive
        for directive in all_directives:
            # Simple directives
            if directive == "noindex":
                directive_flags["has_noindex"] = True
            elif directive == "nofollow":
                directive_flags["has_nofollow"] = True
            elif directive == "none":
                directive_flags["has_none"] = True
            elif directive == "noarchive":
                directive_flags["has_noarchive"] = True
            elif directive == "nosnippet":
                directive_flags["has_nosnippet"] = True
            elif directive == "notranslate":
                directive_flags["has_notranslate"] = True
            elif directive == "noimageindex":
                directive_flags["has_noimageindex"] = True
            elif directive == "index":
                directive_flags["has_index"] = True
            elif directive == "follow":
                directive_flags["has_follow"] = True
            elif directive == "all":
                directive_flags["has_all"] = True
            # Directives with parameters
            elif "unavailable_after:" in directive:
                directive_flags["has_unavailable_after"] = True
            elif "max-snippet:" in directive:
                directive_flags["has_max_snippet"] = True
            elif "max-image-preview:" in directive:
                directive_flags["has_max_image_preview"] = True
            elif "max-video-preview:" in directive:
                directive_flags["has_max_video_preview"] = True

        # Handle defaults: if noindex is not specified, index is implied (unless none is present)
        if not directive_flags["has_noindex"] and not directive_flags["has_none"] and not directive_flags["has_index"]:
            directive_flags["has_index"] = True

        # Handle defaults: if nofollow is not specified, follow is implied (unless none is present)
        if not directive_flags["has_nofollow"] and not directive_flags["has_none"] and not directive_flags["has_follow"]:
            directive_flags["has_follow"] = True

        return directive_flags

    def _count_rel_attributes(self, soup):
        """Count links with specific rel attributes.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Counts of links with specific rel attributes.
        """
        # Initialize counts
        link_counts = {
            "nofollow_links_count": 0,
            "sponsored_links_count": 0,
            "ugc_links_count": 0
        }

        # Find all links
        links = soup.find_all("a", href=True)

        for link in links:
            rel_attr = link.get("rel")

            # BeautifulSoup might return a list or a string for the rel attribute
            if rel_attr:
                # If it's a string, check if it contains the relevant values
                if isinstance(rel_attr, str):
                    rel_values = rel_attr.lower().split()
                # If it's already a list, just use it
                else:
                    rel_values = [r.lower() for r in rel_attr]

                # Count links with specific rel attributes
                if "nofollow" in rel_values:
                    link_counts["nofollow_links_count"] += 1
                if "sponsored" in rel_values:
                    link_counts["sponsored_links_count"] += 1
                if "ugc" in rel_values:
                    link_counts["ugc_links_count"] += 1

        return link_counts

    def _extract_directive_values(self, all_directives):
        """Extract values for directives with parameters.

        Args:
            all_directives (list): List of all directives.

        Returns:
            dict: Dictionary of directive values.
        """
        directive_values = {
            "unavailable_after_date": None,
            "max_snippet_value": None,
            "max_image_preview_value": None,
            "max_video_preview_value": None
        }

        for directive in all_directives:
            # Extract unavailable_after date
            if "unavailable_after:" in directive:
                match = re.search(r'unavailable_after:\s*(.+)', directive)
                if match:
                    directive_values["unavailable_after_date"] = match.group(1).strip()

            # Extract max-snippet value
            elif "max-snippet:" in directive:
                match = re.search(r'max-snippet:\s*(\-*\d+)', directive)
                if match:
                    directive_values["max_snippet_value"] = match.group(1).strip()

            # Extract max-image-preview value
            elif "max-image-preview:" in directive:
                match = re.search(r'max-image-preview:\s*(\w+)', directive)
                if match:
                    directive_values["max_image_preview_value"] = match.group(1).strip()

            # Extract max-video-preview value
            elif "max-video-preview:" in directive:
                match = re.search(r'max-video-preview:\s*(\-*\d+)', directive)
                if match:
                    directive_values["max_video_preview_value"] = match.group(1).strip()

        return directive_values

    def _check_directive_conflicts(self, directive_flags):
        """Check for conflicts between directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.

        Returns:
            bool: True if there are conflicts, False otherwise.
        """
        conflicts = False

        # Check for index/noindex conflict
        if directive_flags["has_index"] and directive_flags["has_noindex"]:
            conflicts = True

        # Check for follow/nofollow conflict
        if directive_flags["has_follow"] and directive_flags["has_nofollow"]:
            conflicts = True

        # Check for all vs. noindex/nofollow conflict
        if directive_flags["has_all"] and (directive_flags["has_noindex"] or
                                          directive_flags["has_nofollow"] or
                                          directive_flags["has_none"]):
            conflicts = True

        return conflicts

    def _analyze_directive_issues(self, directive_flags, has_conflicts):
        """Analyze issues with directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.
            has_conflicts (bool): Whether there are conflicts between directives.

        Returns:
            list: List of identified issues.
        """
        issues = []

        # Check for conflicts
        if has_conflicts:
            issues.append("conflicting_directives")

        # Check for redundant directives (index and follow are default)
        if directive_flags["has_index"] and not directive_flags["has_noindex"] and not directive_flags["has_none"]:
            issues.append("redundant_directives")
        if directive_flags["has_follow"] and not directive_flags["has_nofollow"] and not directive_flags["has_none"]:
            issues.append("redundant_directives")

        # Check for noindex without nofollow (might waste crawl budget)
        if directive_flags["has_noindex"] and not directive_flags["has_nofollow"] and not directive_flags["has_none"]:
            issues.append("noindex_without_nofollow")

        return issues

    def _determine_indexability(self, directive_flags):
        """Determine overall indexability based on directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.

        Returns:
            bool: True if the page is indexable, False otherwise.
        """
        # Not indexable if noindex or none is present
        return not (directive_flags["has_noindex"] or directive_flags["has_none"])

    def _determine_followability(self, directive_flags):
        """Determine overall followability based on directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.

        Returns:
            bool: True if links on the page should be followed, False otherwise.
        """
        # Not followable if nofollow or none is present
        return not (directive_flags["has_nofollow"] or directive_flags["has_none"])


def main():
    """Main entry point for the Directives Parser Worker."""
    parser = argparse.ArgumentParser(description="Directives Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = DirectivesWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()