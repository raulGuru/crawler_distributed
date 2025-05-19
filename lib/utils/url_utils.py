#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
URL processing utilities for the crawler system.

This module provides functions for URL normalization, canonicalization,
duplicate detection, and filtering. It helps ensure consistent URL
handling throughout the crawler system.
"""

import logging
import re
import hashlib
from urllib.parse import (
    urlparse, urlunparse, parse_qs, urlencode,
    urljoin, quote, unquote
)
from posixpath import normpath

logger = logging.getLogger(__name__)

# Commonly excluded URL parameters that don't affect content
_EXCLUDED_PARAMS = {
    # Analytics parameters
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'msclkid', 'dclid', 'zanpid', 'igshid',

    # Session and tracking
    'session_id', 'sid', 'user_id', 'uid', 'visitor_id',

    # Display parameters
    'view', 'mode', 'sort', 'order', 'display', 'layout',

    # Other common parameters
    'ref', 'referrer', 'source', 'origin', 'redirect_to',
}

# Media file extensions to skip
_MEDIA_EXTENSIONS = {
    # Images
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff',

    # Documents
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.csv', '.odt',
    '.ods', '.odp', '.rtf', '.txt',

    # Archives
    '.zip', '.rar', '.gz', '.tar', '.7z', '.bz2', '.iso',

    # Audio/Video
    '.mp3', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.wav', '.ogg', '.aac',
    '.mkv', '.3gp', '.m4a', '.m4v',

    # Other
    '.css', '.js', '.json', '.xml', '.rss', '.atom', '.swf', '.exe', '.dll',
    '.apk', '.dmg', '.pkg', '.deb', '.rpm',
}

_PSEUDO_CCTLD_LABELS = {"co", "com", "net", "org", "gov", "edu", "ac"}



def normalize_url(url, remove_default_port=True, sort_query=True,
                 remove_fragments=True, remove_tracking=True):
    """
    Normalize a URL to a canonical form.

    This function performs various normalization operations:
    - Convert to lowercase
    - Remove default ports (80 for HTTP, 443 for HTTPS)
    - Sort query parameters
    - Remove fragments
    - Remove tracking parameters
    - Handle URL encoding/decoding consistently
    - Normalize paths (resolve '../' and './')

    Args:
        url (str): URL to normalize
        remove_default_port (bool): Whether to remove default ports
        sort_query (bool): Whether to sort query parameters
        remove_fragments (bool): Whether to remove fragments
        remove_tracking (bool): Whether to remove tracking parameters

    Returns:
        str: Normalized URL
    """
    if not url:
        return ''

    try:
        # Parse the URL
        parsed = urlparse(url)

        # Lowercase the scheme and netloc (domain)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Remove default ports
        if remove_default_port:
            if (scheme == 'http' and netloc.endswith(':80')) or \
               (scheme == 'https' and netloc.endswith(':443')):
                netloc = netloc.rsplit(':', 1)[0]

        # Normalize path - resolve '..' and '.'
        path = normpath(parsed.path)

        # Ensure path starts with '/'
        if path and not path.startswith('/'):
            path = '/' + path

        # Handle query parameters
        if parsed.query:
            # Parse query string
            query_params = parse_qs(parsed.query, keep_blank_values=True)

            # Remove tracking parameters if requested
            if remove_tracking:
                query_params = {k: v for k, v in query_params.items()
                               if k.lower() not in _EXCLUDED_PARAMS}

            # Sort parameters if requested
            if sort_query:
                # Build sorted query string
                query = urlencode(sorted(query_params.items()), doseq=True)
            else:
                query = urlencode(query_params, doseq=True)
        else:
            query = ''

        # Handle fragment
        fragment = '' if remove_fragments else parsed.fragment

        # Reconstruct the URL
        normalized_url = urlunparse((scheme, netloc, path, parsed.params, query, fragment))

        return normalized_url

    except Exception as e:
        logger.warning(f"Error normalizing URL {url}: {e}")
        return url


def url_fingerprint(url, include_query=True):
    """
    Generate a fingerprint for a URL to detect duplicates.

    This creates a hash of the normalized URL that can be used
    for efficient duplicate detection.

    Args:
        url (str): URL to fingerprint
        include_query (bool): Whether to include query parameters in the fingerprint

    Returns:
        str: URL fingerprint (SHA-256 hash)
    """
    try:
        # Parse the URL
        parsed = urlparse(url)

        # Normalize components for fingerprinting
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Remove default ports
        if (scheme == 'http' and netloc.endswith(':80')) or \
           (scheme == 'https' and netloc.endswith(':443')):
            netloc = netloc.rsplit(':', 1)[0]

        # Normalize path
        path = normpath(parsed.path)
        if path and not path.startswith('/'):
            path = '/' + path

        # Include query if requested
        if include_query and parsed.query:
            query_params = parse_qs(parsed.query, keep_blank_values=True)
            # Remove tracking parameters
            query_params = {k: v for k, v in query_params.items()
                           if k.lower() not in _EXCLUDED_PARAMS}
            query = urlencode(sorted(query_params.items()), doseq=True)
        else:
            query = ''

        # Build fingerprint string (without fragment)
        fingerprint_base = f"{scheme}://{netloc}{path}"
        if query:
            fingerprint_base += f"?{query}"

        # Create hash
        return hashlib.sha256(fingerprint_base.encode('utf-8')).hexdigest()

    except Exception as e:
        logger.warning(f"Error generating fingerprint for URL {url}: {e}")
        return hashlib.sha256(url.encode('utf-8')).hexdigest()


def is_same_domain(url1, url2, include_subdomains=True):
    """
    Check if two URLs belong to the same domain.

    Args:
        url1 (str): First URL
        url2 (str): Second URL
        include_subdomains (bool): If True, considers subdomains as same domain

    Returns:
        bool: True if URLs belong to the same domain
    """
    try:
        parsed1 = urlparse(url1)
        parsed2 = urlparse(url2)

        domain1 = parsed1.netloc.lower()
        domain2 = parsed2.netloc.lower()

        # Strip default ports if present
        if domain1.endswith(':80') or domain1.endswith(':443'):
            domain1 = domain1.rsplit(':', 1)[0]
        if domain2.endswith(':80') or domain2.endswith(':443'):
            domain2 = domain2.rsplit(':', 1)[0]

        if include_subdomains:
            # Extract base domain (ignoring subdomains)
            base_domain1 = extract_base_domain(domain1)
            base_domain2 = extract_base_domain(domain2)
            return base_domain1 == base_domain2
        else:
            # Exact domain match
            return domain1 == domain2

    except Exception as e:
        logger.warning(f"Error comparing domains {url1} and {url2}: {e}")
        return False


def extract_base_domain(domain):
    """
    Extract the base domain from a domain string.

    For example, 'news.example.com' -> 'example.com'

    Args:
        domain (str): Domain to process

    Returns:
        str: Base domain
    """
    parts = domain.split('.')

    # Handle special cases like co.uk, com.au
    if len(parts) > 2:
        if (parts[-2] == 'co' or parts[-2] == 'com' or parts[-2] == 'org' or
            parts[-2] == 'net' or parts[-2] == 'gov' or parts[-2] == 'edu') and \
           len(parts[-1]) == 2:  # Country code
            return '.'.join(parts[-3:])

    # Extract the main domain (e.g., example.com)
    if len(parts) > 2:
        return '.'.join(parts[-2:])
    else:
        return domain

def extract_base_domain2(url_or_host: str) -> str:
    """
    Best-effort parent domain without external deps.
    Still fails for exotic PSL entries but covers common cases.
    """
    host = urlparse(url_or_host).hostname or url_or_host
    host = host.lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]

    parts = host.split(".")
    if len(parts) >= 3 and parts[-2] in _PSEUDO_CCTLD_LABELS and len(parts[-1]) == 2:
        return ".".join(parts[-3:])          # example.co.uk
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def is_media_url(url):
    """
    Check if URL points to a media file that should be skipped.

    Args:
        url (str): URL to check

    Returns:
        bool: True if URL points to a media file
    """
    if not url:
        return False

    try:
        # Parse the URL
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Check if the path has a media extension
        _, ext = path.rsplit('.', 1) if '.' in path else ('', '')
        if ext and f'.{ext}' in _MEDIA_EXTENSIONS:
            return True

        # Check for query parameters that suggest media
        query_params = parse_qs(parsed.query)
        for param in query_params:
            if param.lower() in ('download', 'dl', 'file'):
                return True

        return False

    except Exception:
        return False


def is_valid_url(url):
    """
    Check if a URL is valid.

    This checks for common issues like missing schemes, invalid characters, etc.

    Args:
        url (str): URL to validate

    Returns:
        bool: True if URL is valid
    """
    if not url:
        return False

    # Basic URL validation regex
    url_pattern = re.compile(
        r'^(https?|ftp)://'  # scheme
        r'([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?'  # domain
        r'(/[^/\s]*)*$'  # path
    )

    try:
        # Validate format
        if not url_pattern.match(url):
            return False

        # Parse URL for more detailed validation
        parsed = urlparse(url)

        # Scheme must be http, https, or ftp
        if parsed.scheme not in ('http', 'https', 'ftp'):
            return False

        # Domain must be present
        if not parsed.netloc:
            return False

        return True

    except Exception:
        return False


def extract_urls_from_text(text, base_url=None):
    """
    Extract URLs from text content.

    Args:
        text (str): Text to extract URLs from
        base_url (str): Base URL for resolving relative URLs

    Returns:
        list: List of extracted URLs
    """
    if not text:
        return []

    # Pattern to match absolute URLs
    absolute_pattern = re.compile(r'https?://[^\s\'"<>]+')

    # Find all absolute URLs
    urls = absolute_pattern.findall(text)

    # If a base URL is provided, also find relative URLs
    if base_url:
        # Pattern to match relative URLs
        relative_pattern = re.compile(r'(?<=[\'"\s])/[^\s\'"<>]+')

        # Find all relative URLs and resolve them
        relative_urls = relative_pattern.findall(text)
        for rel_url in relative_urls:
            abs_url = urljoin(base_url, rel_url)
            urls.append(abs_url)

    # Clean up URLs (remove trailing punctuation, etc.)
    cleaned_urls = []
    for url in urls:
        # Remove trailing punctuation
        while url and url[-1] in '.,;:\'")]}':
            url = url[:-1]

        # Skip if empty after cleaning
        if not url:
            continue

        # Normalize and add to results
        cleaned_urls.append(normalize_url(url))

    return list(set(cleaned_urls))  # Remove duplicates


def should_follow_url(url, allowed_domains=None, allowed_patterns=None,
                     excluded_patterns=None, respect_robots=True):
    """
    Determine if a URL should be followed based on various rules.

    Args:
        url (str): URL to check
        allowed_domains (list): List of allowed domains
        allowed_patterns (list): List of regex patterns for allowed URLs
        excluded_patterns (list): List of regex patterns for excluded URLs
        respect_robots (bool): Whether to respect robots.txt

    Returns:
        bool: True if URL should be followed
    """
    if not url:
        return False

    # Check if it's a valid URL
    if not is_valid_url(url):
        return False

    # Check if it's a media URL
    if is_media_url(url):
        return False

    # Parse the URL
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # Check allowed domains
    if allowed_domains:
        if not any(is_same_domain(url, f"http://{allowed_domain}")
                  for allowed_domain in allowed_domains):
            return False

    # Check excluded patterns
    if excluded_patterns:
        for pattern in excluded_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False

    # Check allowed patterns
    if allowed_patterns:
        if not any(re.search(pattern, url, re.IGNORECASE)
                  for pattern in allowed_patterns):
            return False

    return True


def deduplicate_urls(urls):
    """
    Remove duplicate URLs from a list using fingerprinting.

    Args:
        urls (list): List of URLs to deduplicate

    Returns:
        list: Deduplicated list of URLs
    """
    if not urls:
        return []

    seen_fingerprints = set()
    unique_urls = []

    for url in urls:
        fingerprint = url_fingerprint(url)
        if fingerprint not in seen_fingerprints:
            seen_fingerprints.add(fingerprint)
            unique_urls.append(url)

    return unique_urls