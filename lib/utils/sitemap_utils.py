#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Sitemap utilities for parsing and extracting URLs from XML sitemaps.

This module provides functions to locate, fetch, parse and prioritize URLs
from XML sitemaps, handling sitemap indexes and nested sitemaps.
"""

import logging
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from lxml import etree

logger = logging.getLogger(__name__)

# XML namespaces used in sitemaps
SITEMAP_NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    'xhtml': 'http://www.w3.org/1999/xhtml',
    'image': 'http://www.google.com/schemas/sitemap-image/1.1',
    'news': 'http://www.google.com/schemas/sitemap-news/0.9',
    'video': 'http://www.google.com/schemas/sitemap-video/1.1',
}


def locate_sitemap_url(domain, robots_txt_content=None):
    """
    Locate the sitemap URL for a domain.

    First tries to find the sitemap URL in robots.txt, then falls back to
    common sitemap locations.

    Args:
        domain (str): The domain to locate sitemap for
        robots_txt_content (str, optional): Content of robots.txt if already fetched

    Returns:
        str: The URL of the sitemap, or None if not found
    """
    sitemap_urls = []

    # Try to get sitemap URL from robots.txt
    if not robots_txt_content:
        try:
            robots_url = f"https://{domain}/robots.txt"
            response = requests.get(robots_url, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                robots_txt_content = response.text
        except Exception as e:
            logger.warning(f"Error fetching robots.txt for {domain}: {e}")

    # Parse robots.txt for sitemap entries
    if robots_txt_content:
        sitemap_matches = re.finditer(r"(?i)Sitemap:\s*(https?://\S+)", robots_txt_content)
        sitemap_urls.extend(match.group(1).strip() for match in sitemap_matches)

    # If no sitemap found in robots.txt, try common locations
    if not sitemap_urls:
        common_locations = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/sitemap-index.xml",
            f"https://{domain}/sitemapindex.xml",
            f"https://{domain}/sitemap/sitemap.xml",
        ]

        for url in common_locations:
            try:
                response = requests.head(url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    # Make sure to use the final URL after redirects
                    sitemap_urls.append(response.url)
                    break
            except Exception as e:
                logger.debug(f"Error checking sitemap at {url}: {e}")
                continue

    if sitemap_urls:
        return sitemap_urls[0]  # Return the first sitemap URL found

    logger.warning(f"No sitemap found for {domain}")
    return None


def is_sitemap_outdated(sitemap_url, max_age_days=90):
    """
    Check if a sitemap is outdated based on its last-modified header.

    Args:
        sitemap_url (str): URL of the sitemap
        max_age_days (int): Maximum age in days before sitemap is considered outdated

    Returns:
        bool: True if sitemap is outdated, False otherwise
    """
    try:
        response = requests.head(sitemap_url, timeout=5)
        if response.status_code == 200:
            # Check Last-Modified header
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                last_modified_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
                max_age = timedelta(days=max_age_days)
                if datetime.now() - last_modified_date > max_age:
                    logger.info(f"Sitemap {sitemap_url} is outdated (last modified: {last_modified})")
                    return True

                return False
    except Exception as e:
        logger.warning(f"Error checking sitemap age for {sitemap_url}: {e}")

    # If we can't determine age, assume it's not outdated
    return False


def fetch_sitemap(sitemap_url):
    """
    Fetch a sitemap from a URL.

    Args:
        sitemap_url (str): URL of the sitemap to fetch

    Returns:
        str: Content of the sitemap, or None if fetch failed
    """
    try:
        response = requests.get(sitemap_url, timeout=20)
        if response.status_code == 200:
            return response.content
        else:
            logger.warning(f"Failed to fetch sitemap {sitemap_url}: HTTP {response.status_code}")
    except Exception as e:
        logger.warning(f"Error fetching sitemap {sitemap_url}: {e}")

    return None


def extract_urls_from_sitemap_index(index_content, base_url=None):
    """
    Extract sitemap URLs from a sitemap index.

    Args:
        index_content (str): Content of the sitemap index
        base_url (str, optional): Base URL for resolving relative URLs

    Returns:
        list: List of sitemap URLs
    """
    if not index_content:
        return []

    sitemap_urls = []

    try:
        # Parse XML
        root = etree.fromstring(index_content)

        # Extract sitemap URLs
        for sitemap in root.xpath("//sm:sitemap/sm:loc", namespaces=SITEMAP_NS):
            url = sitemap.text.strip()
            if base_url and not url.startswith(('http://', 'https://')):
                url = urljoin(base_url, url)

            sitemap_urls.append(url)
    except Exception as e:
        logger.warning(f"Error parsing sitemap index: {e}")

    return sitemap_urls


def is_sitemap_index(content):
    """
    Check if the XML content is a sitemap index.

    Args:
        content (str or bytes): XML content to check

    Returns:
        bool: True if content is a sitemap index, False otherwise
    """
    if not content:
        return False

    try:
        # Make sure content is bytes
        if isinstance(content, str):
            content = content.encode('utf-8')

        # Parse XML
        root = etree.fromstring(content)

        # Check tag name - two possible ways to detect a sitemap index
        # 1. Check root tag directly
        if root.tag == f"{{{SITEMAP_NS['sm']}}}sitemapindex":
            return True

        # 2. Check for sitemap elements (child sitemaps)
        sitemap_elements = root.xpath("//sm:sitemap", namespaces=SITEMAP_NS)
        if sitemap_elements:
            return True

        return False
    except Exception as e:
        logger.warning(f"Error checking if content is a sitemap index: {e}")
        # Try a simpler string-based check as fallback
        if isinstance(content, bytes):
            content = content.decode('utf-8', errors='replace')
        return '<sitemapindex' in content or '<sitemap>' in content


def extract_urls_from_sitemap(sitemap_content, base_url=None):
    """
    Extract URLs from a sitemap.

    Args:
        sitemap_content (str): Content of the sitemap
        base_url (str, optional): Base URL for resolving relative URLs

    Returns:
        list: List of URL entries with metadata (url, lastmod, priority, changefreq)
    """
    if not sitemap_content:
        return []

    urls = []

    try:
        # Parse XML
        root = etree.fromstring(sitemap_content)

        # Extract URLs
        for url_elem in root.xpath("//sm:url", namespaces=SITEMAP_NS):
            try:
                # Get URL and metadata
                loc = url_elem.xpath("sm:loc", namespaces=SITEMAP_NS)[0].text.strip()

                if base_url and not loc.startswith(('http://', 'https://')):
                    loc = urljoin(base_url, loc)

                # Extract optional metadata
                lastmod = None
                lastmod_elem = url_elem.xpath("sm:lastmod", namespaces=SITEMAP_NS)
                if lastmod_elem:
                    lastmod = lastmod_elem[0].text.strip()

                priority = 0.5  # Default priority
                priority_elem = url_elem.xpath("sm:priority", namespaces=SITEMAP_NS)
                if priority_elem:
                    try:
                        priority = float(priority_elem[0].text.strip())
                    except ValueError:
                        pass

                changefreq = None
                changefreq_elem = url_elem.xpath("sm:changefreq", namespaces=SITEMAP_NS)
                if changefreq_elem:
                    changefreq = changefreq_elem[0].text.strip()

                # Add URL to list
                urls.append({
                    'url': loc,
                    'lastmod': lastmod,
                    'priority': priority,
                    'changefreq': changefreq
                })
            except Exception as e:
                logger.warning(f"Error parsing URL element: {e}")
                continue
    except Exception as e:
        logger.warning(f"Error parsing sitemap: {e}")

    return urls


def prioritize_urls(urls, max_pages=None):
    """
    Prioritize URLs based on metadata.

    Args:
        urls (list): List of URL entries with metadata
        max_pages (int, optional): Maximum number of URLs to return

    Returns:
        list: Prioritized list of URLs (strings only)
    """
    # Calculate scores based on metadata
    for url_entry in urls:
        score = url_entry['priority']  # Start with priority

        # Adjust score based on lastmod (more recent = higher score)
        if url_entry['lastmod']:
            try:
                # Parse lastmod date (handle different formats)
                for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"):
                    try:
                        lastmod_date = datetime.strptime(url_entry['lastmod'], fmt)
                        break
                    except ValueError:
                        continue

                # Calculate days since last modification
                days_ago = (datetime.now() - lastmod_date).days if lastmod_date else None
                if days_ago is not None:
                    # More recent pages get higher score
                    recency_score = max(0, 1 - (days_ago / 365))  # Scale to 0-1 based on last year
                    score += recency_score
            except Exception:
                pass

        # Adjust score based on changefreq
        if url_entry['changefreq']:
            freq_scores = {
                'always': 0.5,
                'hourly': 0.4,
                'daily': 0.3,
                'weekly': 0.2,
                'monthly': 0.1,
                'yearly': 0.05,
                'never': 0
            }
            score += freq_scores.get(url_entry['changefreq'], 0)

        url_entry['score'] = score

    # Sort URLs by score (highest first)
    sorted_urls = sorted(urls, key=lambda x: x['score'], reverse=True)

    # Limit to max_pages if specified
    if max_pages:
        sorted_urls = sorted_urls[:max_pages]

    # Return just the URLs (not the metadata)
    return [entry['url'] for entry in sorted_urls]


def get_urls_from_sitemap(domain, max_pages=None, max_age_days=90):
    """
    Get a prioritized list of URLs from a domain's sitemap.

    Args:
        domain (str): The domain to get URLs for
        max_pages (int, optional): Maximum number of URLs to return
        max_age_days (int): Maximum age of the sitemap in days before it's considered outdated

    Returns:
        tuple: (urls, status) where urls is a list of URLs and status is one of:
               'success', 'outdated', 'not_found', or 'error'
    """
    # Locate sitemap URL
    sitemap_url = locate_sitemap_url(domain)
    if not sitemap_url:
        logger.info(f"No sitemap found for {domain}")
        return [], 'not_found'

    # Check if sitemap is outdated
    if is_sitemap_outdated(sitemap_url, max_age_days):
        logger.info(f"Sitemap for {domain} is outdated")
        return [], 'outdated'

    # Fetch and process sitemap
    all_urls = []
    try:
        # Fetch initial sitemap
        content = fetch_sitemap(sitemap_url)
        if not content:
            return [], 'error'

        # Check if it's a sitemap index
        if is_sitemap_index(content):
            # Process sitemap index
            sitemap_urls = extract_urls_from_sitemap_index(content, sitemap_url)

            # Process each sitemap in the index
            for url in sitemap_urls:
                sitemap_content = fetch_sitemap(url)
                if sitemap_content:
                    urls = extract_urls_from_sitemap(sitemap_content, url)
                    all_urls.extend(urls)

                # Limit the number of processed sitemaps to avoid overloading
                if max_pages and len(all_urls) >= max_pages * 2:
                    break
        else:
            # Process regular sitemap
            all_urls = extract_urls_from_sitemap(content, sitemap_url)

        # Prioritize and limit URLs
        prioritized_urls = prioritize_urls(all_urls, max_pages)

        return prioritized_urls, 'success'
    except Exception as e:
        logger.error(f"Error processing sitemap for {domain}: {e}")
        return [], 'error'