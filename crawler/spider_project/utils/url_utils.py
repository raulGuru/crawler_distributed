#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
URL utilities for the crawler.

This module provides common URL-related functions used across the crawler,
including extension checking, domain parsing, etc.
"""

import os
import re
from urllib.parse import urlparse
from scrapy.utils.project import get_project_settings

# Get settings to access SKIPPED_EXTENSIONS
settings = get_project_settings()

# Get the list of skipped extensions from settings
SKIPPED_EXTENSIONS = settings.getlist('SKIPPED_EXTENSIONS', [
    # images
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico',
    # documents
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx', '.csv',
    # archives
    '.zip', '.rar', '.gz', '.tar', '.7z',
    # audio/video
    '.mp3', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.wma', '.aac', '.ogg',
    # other
    '.css', '.js', '.json', '.rss', '.atom'
])

# Compile a regex pattern for matching skipped extensions
EXTENSION_PATTERN = re.compile(
    r'\.(' + '|'.join(ext.lstrip('.') for ext in SKIPPED_EXTENSIONS) + r')(\?.*)?$',
    re.IGNORECASE
)

def has_skipped_extension(url):
    """
    Check if URL has a skipped file extension.

    Args:
        url (str): URL to check

    Returns:
        bool: True if URL has a skipped extension
    """
    # Check with regex for common patterns
    if EXTENSION_PATTERN.search(url):
        return True

    # Parse URL and check path more thoroughly
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Skip empty paths or directory paths
        if not path or path.endswith('/'):
            return False

        # Get file extension
        _, ext = os.path.splitext(path)
        if ext and ext.lower() in SKIPPED_EXTENSIONS:
            return True
    except Exception:
        pass

    return False

def get_domain_from_url(url):
    """
    Extract domain from URL and normalize it.

    Args:
        url (str): URL to extract domain from

    Returns:
        str: Normalized domain name from the URL
    """
    try:
        parsed = urlparse(url)
        return normalize_domain(parsed.netloc)
    except Exception:
        return None

def normalize_domain(domain: str) -> str:
    """
    Normalize a domain name for consistent folder naming.
    - Strips 'www.' prefix if present
    - Converts to lowercase
    - Strips whitespace
    """
    if not domain:
        return domain
    domain = domain.strip().lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain