"""URI Parser Worker module.

This module contains the UriWorker class which extracts detailed URL analysis
from saved HTML files as part of a distributed crawl-parser system.
"""

import os
import sys
import argparse
import re
from urllib.parse import urlparse, parse_qs, unquote

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class UriWorker(BaseParserWorker):
    """Worker for extracting detailed URI (URL) data from HTML files.

    This worker processes HTML files saved by the crawler and extracts comprehensive
    analysis of the URL structure, components, and characteristics, storing the
    results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the UriWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="crawler_htmlparser_uri_tube",
            task_type="uri",
            instance_id=instance_id,
        )
        # Common tracking and session parameter patterns
        self.tracking_params = [
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'gclid', 'fbclid', 'msclkid', 'ref', 'source', 'campaign'
        ]
        self.session_params = [
            'sid', 'session', 'sessid', 'sessionid', 'token', 'auth'
        ]
        self.sort_filter_params = [
            'sort', 'order', 'filter', 'page', 'limit', 'per_page', 'size',
            'color', 'price', 'category', 'tag'
        ]
        self.special_chars_pattern = re.compile(r'[^\w\-/.]')

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "uri_data"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract URI (URL) specific data for analysis.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).
            url (str): The URL of the page.
            domain (str): The domain of the page.

        Returns:
            dict: Extracted URI data.

        Raises:
            NonRetryableError: For URL parsing errors.
        """
        try:
            # Basic URL information
            decoded_url = unquote(url)
            url_length = len(url)

            # Parse URL into components
            parsed_url = urlparse(url)

            # # Extract URL components
            # TODO: Will be added later
            # components = self._extract_url_components(parsed_url)

            # Analyze URL path
            path_analysis = self._analyze_path(parsed_url.path)

            # Analyze query parameters
            query_analysis = self._analyze_query(parsed_url.query)

            # Analyze URL fragment
            fragment_analysis = self._analyze_fragment(parsed_url.fragment)

            # Analyze SEO characteristics
            seo_characteristics = self._analyze_seo_characteristics(
                parsed_url,
                url,
                html_content,
                path_analysis
            )

            # Detect mobile-specific URL patterns
            mobile_characteristics = self._detect_mobile_characteristics(url)

            # Identify issues with URL structure
            issues = self._identify_url_issues(
                url,
                #components,  # TODO: Will be added later
                path_analysis,
                query_analysis,
                seo_characteristics
            )

            # Assemble the complete URI data
            uri_data = {
                "url": url,
                "decoded_url": decoded_url,
                "length": url_length,
                #"components": components,  # TODO: Will be added later
                "path_analysis": path_analysis,
                "query_analysis": query_analysis,
                "fragment_analysis": fragment_analysis,
                "seo_characteristics": seo_characteristics,
                "mobile_characteristics": mobile_characteristics,
                "issues": issues
            }

            self.logger.debug(
                f"Extracted uri_data for URL: {url}, doc_id: {doc_id_str}"
            )

            return uri_data

        except Exception as e:
            self.logger.error(
                f"Failed to extract URI data for {html_path}, URL: {url}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"Failed to extract URI data for {url}: {e}")

    def _extract_url_components(self, parsed_url):
        """Extract components from a parsed URL.

        Args:
            parsed_url: The result of urlparse()

        Returns:
            dict: Dictionary of URL components
        """
        # Get netloc (domain + port)
        netloc = parsed_url.netloc

        # Extract domain information
        domain_parts = parsed_url.netloc.split(':')[0].split('.')

        # Handle domain extraction edge cases
        if len(domain_parts) >= 2:
            tld = domain_parts[-1]
            domain = domain_parts[-2]

            # Handle cases like co.uk, com.au
            if len(domain_parts) > 2 and len(domain_parts[-2]) <= 3 and len(domain_parts[-1]) <= 3:
                tld = f"{domain_parts[-2]}.{domain_parts[-1]}"
                domain = domain_parts[-3]
                subdomain = '.'.join(domain_parts[:-3]) if len(domain_parts) > 3 else None
            else:
                subdomain = '.'.join(domain_parts[:-2]) if len(domain_parts) > 2 else None
        else:
            # Handle localhost or IP addresses
            domain = netloc
            tld = None
            subdomain = None

        return {
            "scheme": parsed_url.scheme,
            "netloc": netloc,
            "domain": domain,
            "subdomain": subdomain,
            "tld": tld,
            "path": parsed_url.path,
            "query": parsed_url.query if parsed_url.query else None,
            "fragment": parsed_url.fragment if parsed_url.fragment else None,
            "port": parsed_url.port,
            "username": parsed_url.username,
            "password": parsed_url.password
        }

    def _analyze_path(self, path):
        """Analyze URL path components.

        Args:
            path (str): The URL path

        Returns:
            dict: Path analysis information
        """
        # Split path into segments
        segments = [segment for segment in path.split('/') if segment]

        # Compute path metrics
        path_length = len(path)
        segment_count = len(segments)
        directory_depth = len(segments) - 1 if segments and '.' in segments[-1] else len(segments)

        # Detect file extension
        file_extension = None
        if segments and '.' in segments[-1]:
            file_extension = segments[-1].split('.')[-1]

        # Check for formatting issues
        contains_uppercase = any(c.isupper() for c in path)
        contains_spaces = ' ' in path
        contains_underscores = '_' in path
        contains_special_chars = bool(self.special_chars_pattern.search(path))
        trailing_slash = path.endswith('/') and len(path) > 1

        return {
            "path_length": path_length,
            "segments": segments,
            "segment_count": segment_count,
            "directory_depth": directory_depth,
            "file_extension": file_extension,
            "trailing_slash": trailing_slash,
            "contains_uppercase": contains_uppercase,
            "contains_spaces": contains_spaces,
            "contains_underscores": contains_underscores,
            "contains_special_chars": contains_special_chars
        }

    def _analyze_query(self, query):
        """Analyze URL query parameters.

        Args:
            query (str): The URL query string

        Returns:
            dict: Query analysis information
        """
        has_query = bool(query)
        parameters = {}
        tracking_parameters = []
        session_parameters = []
        sort_filter_parameters = []

        if has_query:
            # Parse query parameters
            query_params = parse_qs(query)

            # Process each parameter
            for param, values in query_params.items():
                param_lower = param.lower()
                parameters[param] = values[0] if len(values) == 1 else values

                # Check for tracking parameters
                if any(tracker in param_lower for tracker in self.tracking_params):
                    tracking_parameters.append(param)

                # Check for session parameters
                if any(session in param_lower for session in self.session_params):
                    session_parameters.append(param)

                # Check for sort/filter parameters
                if any(sort_filter in param_lower for sort_filter in self.sort_filter_params):
                    sort_filter_parameters.append(param)

        return {
            "has_query_string": has_query,
            "parameter_count": len(parameters),
            "parameters": parameters,
            "tracking_parameters": tracking_parameters,
            "session_parameters": session_parameters,
            "sort_filter_parameters": sort_filter_parameters
        }

    def _analyze_fragment(self, fragment):
        """Analyze URL fragment (hash).

        Args:
            fragment (str): The URL fragment

        Returns:
            dict: Fragment analysis information
        """
        has_fragment = bool(fragment)
        fragment_value = fragment if has_fragment else None
        is_hashbang = fragment.startswith('!') if has_fragment else False

        return {
            "has_fragment": has_fragment,
            "fragment_value": fragment_value,
            "is_hashbang": is_hashbang
        }

    def _analyze_seo_characteristics(self, parsed_url, url, html_content, path_analysis):
        """Analyze SEO characteristics of the URL.

        Args:
            parsed_url: Parsed URL object
            url (str): The original URL
            html_content (str): The HTML content
            path_analysis (dict): Path analysis results

        Returns:
            dict: SEO characteristics
        """
        # Extract keywords from URL path segments
        keywords = []
        for segment in path_analysis.get("segments", []):
            # Split by common separators and get words
            parts = re.split(r'[-_.]', segment)
            keywords.extend([
                part.lower() for part in parts
                if part and len(part) > 2 and part.lower() not in [
                    'the', 'and', 'for', 'with', 'from', 'that', 'this',
                    'are', 'was', 'were', 'will', 'have', 'has', 'page',
                    'html', 'php', 'asp', 'jsp'
                ]
            ])

        # Calculate URL readability score (0-100)
        readability_score = self._calculate_url_readability(url, path_analysis)

        # Check for canonical URL in HTML
        canonical_url = None
        is_canonical = False
        soup = self._create_soup(html_content)
        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get("href"):
            canonical_url = canonical_tag.get("href")
            is_canonical = canonical_url == url

        # Check for protocol-relative URLs in HTML
        has_protocol_relative_url = False
        if '"//' in html_content or "'//" in html_content:
            has_protocol_relative_url = True

        return {
            "is_seo_friendly": self._is_seo_friendly(url, path_analysis, readability_score),
            "contains_keywords": keywords,
            "url_readability_score": readability_score,
            "is_canonical": is_canonical,
            "has_protocol_relative_url": has_protocol_relative_url,
            "is_https": parsed_url.scheme == 'https'
        }

    def _calculate_url_readability(self, url, path_analysis):
        """Calculate a readability score for the URL.

        Args:
            url (str): The URL
            path_analysis (dict): Path analysis results

        Returns:
            int: Readability score (0-100)
        """
        score = 100

        # Penalize for non-SEO-friendly characteristics
        if path_analysis["contains_uppercase"]:
            score -= 15

        if path_analysis["contains_spaces"]:
            score -= 15

        if path_analysis["contains_underscores"]:
            score -= 10

        if path_analysis["contains_special_chars"]:
            score -= 15

        # Query string penalties
        has_query = "?" in url
        if has_query:
            query_parts = url.split('?')[1].split('&')
            parameter_penalty = min(len(query_parts) * 5, 25)
            score -= parameter_penalty

        # Fragment penalty
        if "#" in url:
            score -= 5

        # URL length penalties
        url_length = len(url)
        if url_length > 100:
            length_penalty = min((url_length - 100) // 20, 15)
            score -= length_penalty

        # Depth penalty
        depth_penalty = min(path_analysis["directory_depth"] * 3, 15)
        score -= depth_penalty

        # Ensure score stays within 0-100
        return max(0, min(100, score))

    def _is_seo_friendly(self, url, path_analysis, readability_score):
        """Determine if a URL is SEO friendly.

        Args:
            url (str): The URL
            path_analysis (dict): Path analysis results
            readability_score (int): URL readability score

        Returns:
            bool: True if URL is SEO friendly
        """
        return (
            readability_score >= 70 and
            not path_analysis["contains_uppercase"] and
            not path_analysis["contains_spaces"] and
            not path_analysis["contains_special_chars"] and
            url.startswith("https://")
        )

    def _detect_mobile_characteristics(self, url):
        """Detect mobile-specific URL patterns.

        Args:
            url (str): The URL

        Returns:
            dict: Mobile characteristics
        """
        # Check for AMP URL patterns
        # TODO: Will be added later
        # amp_pattern = r'(\/amp\/|\.amp$|amp=1|\/amp$)'
        # is_amp_url = bool(re.search(amp_pattern, url))

        # Check for mobile-specific URL patterns
        mobile_pattern = r'(^m\.|\/m\/|^mobile\.|\/mobile\/)'
        is_mobile_url = bool(re.search(mobile_pattern, url))

        return {
            #"is_amp_url": is_amp_url,  # TODO: Will be added later
            "is_mobile_url": is_mobile_url
        }

    def _identify_url_issues(self, url, components, path_analysis, query_analysis, seo_characteristics):
        """Identify issues with URL structure.

        Args:
            url (str): The URL
            components (dict): URL components
            path_analysis (dict): Path analysis
            query_analysis (dict): Query analysis
            seo_characteristics (dict): SEO characteristics

        Returns:
            list: Issues identified
        """
        issues = []

        # Check URL length
        if len(url) > 2048:
            issues.append("excessive_url_length")
        elif len(url) > 1000:
            issues.append("long_url")

        # Check path depth
        if path_analysis["directory_depth"] > 5:
            issues.append("excessive_directory_depth")

        # Check formatting issues
        if path_analysis["contains_uppercase"]:
            issues.append("uppercase_in_url")

        if path_analysis["contains_spaces"]:
            issues.append("spaces_in_url")

        if path_analysis["contains_underscores"]:
            issues.append("underscores_in_url")

        if path_analysis["contains_special_chars"]:
            issues.append("special_chars_in_url")

        # Check protocol
        # TODO: Will be added later
        # if components["scheme"] != "https":
        #     issues.append("not_using_https")

        # Check credentials in URL
        # TODO: Will be added later
        # if components["username"] or components["password"]:
        #     issues.append("credentials_in_url")

        # Check for problematic file extensions
        problematic_extensions = ['exe', 'zip', 'rar', 'doc', 'docx', 'pdf', 'xls', 'xlsx', 'ppt', 'pptx']
        if path_analysis["file_extension"] in problematic_extensions:
            issues.append("contains_document_file_extension")

        # Check for tracking parameters
        if query_analysis["tracking_parameters"]:
            issues.append("contains_tracking_parameters")

        # Check for session parameters
        if query_analysis["session_parameters"]:
            issues.append("contains_session_parameters")

        # Check for excessive query parameters
        if query_analysis["parameter_count"] > 4:
            issues.append("excessive_query_parameters")

        # Check for protocol-relative URLs
        if seo_characteristics["has_protocol_relative_url"]:
            issues.append("contains_protocol_relative_urls")

        # Check if URL is canonical
        if not seo_characteristics["is_canonical"]:
            issues.append("not_canonical_version")

        return issues


def main():
    """Main entry point for the URI Data Parser Worker."""
    parser = argparse.ArgumentParser(description="URI Data Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = UriWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()