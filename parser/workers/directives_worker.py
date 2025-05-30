"""Directives Parser Worker module.

This module contains the DirectivesWorker class which extracts robots directives
from saved HTML files as part of a distributed crawl-parser system.
"""


import os
import sys
import re
import argparse
import json

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
    from meta tags, HTTP headers (X-Robots-Tag), and analyzes their implications for search engines.
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
        """Extract directives data from HTML content and HTTP headers.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted directives data.

        Raises:
            NonRetryableError: For HTML parsing errors or critical data loading issues.
        """
        try:
            soup = self._create_soup(html_content)

            headers_file_path = self.job_data.get('headers_file_path')
            response_headers = self._load_headers_from_file(headers_file_path)
            if not response_headers:
                response_headers = {}


            # Extract meta robots and googlebot tags
            meta_robots_tags_content = self._extract_meta_tags_content(soup, "robots")
            meta_googlebot_tags_content = self._extract_meta_tags_content(soup, "googlebot")

            meta_robots_directives = self._parse_directives_from_content_list(meta_robots_tags_content)
            meta_googlebot_directives = self._parse_directives_from_content_list(meta_googlebot_tags_content)


            # Extract X-Robots-Tag from loaded HTTP headers
            # Header keys are stored as lowercase by the spider.
            x_robots_tag_header_values = response_headers.get('x-robots-tag', [])
            if not isinstance(x_robots_tag_header_values, list): # Ensure it's a list
                x_robots_tag_header_values = [x_robots_tag_header_values]

            x_robots_tag_directives = self._parse_directives_from_content_list(x_robots_tag_header_values)

            # For reporting, store the raw X-Robots-Tag header string(s)
            raw_x_robots_tag_string = ", ".join(x_robots_tag_header_values) if x_robots_tag_header_values else None


            # Get all directives from meta robots, googlebot tags, and X-Robots-Tag
            all_directives = self._combine_directives(
                meta_robots_directives,
                meta_googlebot_directives,
                x_robots_tag_directives
            )

            # Check for the presence of specific directives
            directive_flags = self._analyze_directive_presence(all_directives)

            # Count links with specific rel attributes
            link_counts = self._count_rel_attributes(soup)

            # Extract values for directives with parameters
            directive_values = self._extract_directive_values(all_directives)

            # Check for conflicts between directives
            has_conflicts = self._check_directive_conflicts(directive_flags)

            # Analyze issues with directives
            issues = self._analyze_directive_issues(directive_flags, has_conflicts, meta_robots_directives, x_robots_tag_directives)

            # Determine overall indexability and followability
            indexability = self._determine_indexability(directive_flags)
            followability = self._determine_followability(directive_flags)

            # Construct the final directives data
            directives_data = {
                "meta_robots": meta_robots_tags_content,
                "meta_googlebot": meta_googlebot_tags_content,
                "x_robots_tag": raw_x_robots_tag_string,
                "all_parsed_directives": all_directives,
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
                f"Directive extraction failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"Directive extraction failed for {html_path}: {e}")

    def _extract_meta_tags_content(self, soup, name: str) -> list[str]:
        """Extract content strings from meta tags with the specified name.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            name (str): Name attribute of the meta tag to extract.

        Returns:
            list[str]: List of content strings found in the meta tags.
        """
        contents = []
        meta_tags = soup.find_all("meta", attrs={"name": name})
        for tag in meta_tags:
            content = tag.get("content", "").strip()
            if content:
                contents.append(content)
        return contents

    def _parse_directives_from_content_list(self, content_list: list[str]) -> list[str]:
        """Parses a list of content strings into a flat list of directives.
           Example: ["noindex, nofollow", "max-snippet:10"] -> ["noindex", "nofollow", "max-snippet:10"]
        """
        directives = []
        for content_string in content_list:
            # Split by comma and strip whitespace, convert to lowercase
            tag_directives = [d.strip().lower() for d in content_string.split(",")]
            directives.extend(d for d in tag_directives if d) # Ensure no empty strings
        return directives

    def _combine_directives(self, meta_robots_directives, meta_googlebot_directives, x_robots_directives):
        """Combine directives from different sources.

        Args:
            meta_robots_directives (list): Parsed directives from meta robots tags.
            meta_googlebot_directives (list): Parsed directives from meta googlebot tags.
            x_robots_directives (list): Parsed directives from X-Robots-Tag HTTP header.

        Returns:
            list: Combined list of unique directives (all lowercase).
        """
        combined = []
        combined.extend(meta_robots_directives)
        combined.extend(meta_googlebot_directives)
        combined.extend(x_robots_directives)

        # Remove duplicates while preserving order (important for specificity if ever needed)
        # and ensure all are lowercase.
        unique_directives = []
        seen_directives = set()
        for directive in combined:
            # Directives like max-snippet:10 should be kept as is, but noindex should be lowercase
            # The parsing in _parse_directives_from_content_list already lowercases non-value parts
            # For now, we assume directives are already appropriately cased/lowercased by the parser.
            # Screaming Frog seems to treat most directives case-insensitively.
            # We are already doing .lower() in _parse_directives_from_content_list
            if directive not in seen_directives:
                unique_directives.append(directive)
                seen_directives.add(directive)
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
            "has_noimageindex": False,
            "has_index": False,
            "has_follow": False,
            # TODO: Will be added later
            # "has_none": False,
            # "has_noarchive": False,
            # "has_nosnippet": False,
            # "has_notranslate": False,
            # "has_unavailable_after": False,
            # "has_max_snippet": False,
            # "has_max_image_preview": False,
            # "has_max_video_preview": False,
            # "has_all": False
        }

        # Check for each directive
        for directive in all_directives:
            # Simple directives
            if directive == "noindex":
                directive_flags["has_noindex"] = True
            elif directive == "nofollow":
                directive_flags["has_nofollow"] = True
            elif directive == "index":
                directive_flags["has_index"] = True
            elif directive == "follow":
                directive_flags["has_follow"] = True
            # TODO: Will be added later
            # elif directive == "none":
            #     directive_flags["has_none"] = True
            # elif directive == "noarchive":
            #     directive_flags["has_noarchive"] = True
            # elif directive == "nosnippet":
            #     directive_flags["has_nosnippet"] = True
            # elif directive == "notranslate":
            #     directive_flags["has_notranslate"] = True
            # elif directive == "noimageindex":
            #     directive_flags["has_noimageindex"] = True
            # elif directive == "all":
            #     directive_flags["has_all"] = True
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
        if not directive_flags["has_noindex"] and not directive_flags["has_index"]: #and not directive_flags["has_none"]:
            directive_flags["has_index"] = True

        # Handle defaults: if nofollow is not specified, follow is implied (unless none is present)
        if not directive_flags["has_nofollow"] and not directive_flags["has_follow"]: #and not directive_flags["has_none"]:
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
            # TODO: Will be added later
            # "max_snippet_value": None,
            # "max_image_preview_value": None,
            # "max_video_preview_value": None
        }

        for directive in all_directives:
            # Extract unavailable_after date
            if "unavailable_after:" in directive:
                match = re.search(r'unavailable_after:\s*(.+)', directive)
                if match:
                    directive_values["unavailable_after_date"] = match.group(1).strip()

            # TODO: Will be added later
            # Extract max-snippet value
            # elif "max-snippet:" in directive:
            #     match = re.search(r'max-snippet:\s*(\-*\d+)', directive)
            #     if match:
            #         directive_values["max_snippet_value"] = match.group(1).strip()

            # # Extract max-image-preview value
            # elif "max-image-preview:" in directive:
            #     match = re.search(r'max-image-preview:\s*(\w+)', directive)
            #     if match:
            #         directive_values["max_image_preview_value"] = match.group(1).strip()

            # # Extract max-video-preview value
            # elif "max-video-preview:" in directive:
            #     match = re.search(r'max-video-preview:\s*(\-*\d+)', directive)
            #     if match:
            #         directive_values["max_video_preview_value"] = match.group(1).strip()

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
        #if directive_flags["has_all"] and (directive_flags["has_noindex"] or directive_flags["has_nofollow"]) or directive_flags["has_none"]):
        if (directive_flags["has_noindex"] or directive_flags["has_nofollow"]):
            conflicts = True

        return conflicts

    def _analyze_directive_issues(self, directive_flags, has_conflicts, meta_directives, header_directives):
        """Analyze issues with directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.
            has_conflicts (bool): Whether there are conflicts between directives.
            meta_directives (list): Directives found in meta tags.
            header_directives (list): Directives found in X-Robots-Tag.

        Returns:
            list: List of identified issues.
        """
        issues = []

        # Check for conflicts
        if has_conflicts:
            issues.append("conflicting_directives")

        # Check for redundant directives (index and follow are default if not overridden)
        # A directive like "index" is only redundant if "noindex" and "none" are absent.
        if directive_flags["has_index"] and not directive_flags["has_noindex"]: #and not directive_flags["has_none"]:
            # Check if "index" was explicitly stated or just default.
            # If "index" is in all_directives, it means it was explicitly stated.
            # Note: all_directives comes from combining meta, googlebot, and x-robots.
            # We need to check if 'index' was *explicitly* stated or just the default.
            # The 'has_index' flag is set to True by default if noindex/none are missing.
            # An explicit "index" is redundant.
            # For this check, we need to see if "index" was actually in the source directives.
            pass # Redundancy of "index" or "follow" is often minor, Screaming Frog might not flag it heavily.


        # Check for noindex without nofollow (might waste crawl budget)
        if directive_flags["has_noindex"] and not directive_flags["has_nofollow"]: #and not directive_flags["has_none"]:
            issues.append("noindex_without_nofollow")

        # Check for different directives from meta tags vs. HTTP headers if both exist
        if meta_directives and header_directives:
            # Normalize by sorting for comparison, as order within a single source doesn't matter for presence.
            # However, the combination of directives from ALL sources matters.
            # This check is more about whether meta and header provide conflicting signals *if looked at in isolation*.
            # A more robust check for conflicting sources would be to see if the *effective outcome* differs.
            # For example, meta says "noindex", header says "index". That's a conflict.
            # Screaming Frog reports "Meta Robots 1" and "X-Robots-Tag 1" separately.
            # The "Indexability Status" then gives the reason.
            # For now, we don't add an issue here, as `has_conflicts` already covers combined effective conflicts.
            pass


        return issues

    def _determine_indexability(self, directive_flags):
        """Determine overall indexability based on directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.

        Returns:
            bool: True if the page is indexable, False otherwise.
        """
        # Not indexable if noindex or none is present
        return not (directive_flags["has_noindex"]) #or directive_flags["has_none"])

    def _determine_followability(self, directive_flags):
        """Determine overall followability based on directives.

        Args:
            directive_flags (dict): Dictionary of directive presence flags.

        Returns:
            bool: True if links on the page should be followed, False otherwise.
        """
        # Not followable if nofollow or none is present
        return not (directive_flags["has_nofollow"]) #or directive_flags["has_none"])


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