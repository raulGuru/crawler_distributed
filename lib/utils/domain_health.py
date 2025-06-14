#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Domain connectivity health check utilities."""

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


def _fetch_status(url: str, timeout: float = 5.0) -> Optional[int]:
    """Fetch a URL and return the HTTP status code.

    Args:
        url: URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        HTTP status code if the request succeeds, otherwise ``None``.
    """
    try:
        response = requests.get(url, timeout=timeout, allow_redirects=True)
        return response.status_code
    except requests.RequestException as e:
        logger.debug("Request to %s failed: %s", url, e)
        return None


def check_domain_connectivity(domain: str) -> Dict[str, Optional[int]]:
    """Check basic connectivity to a domain.

    Performs lightweight ``GET`` requests to the domain root and its
    ``robots.txt``. A short timeout is used to avoid blocking.

    Args:
        domain: Domain to check (without scheme).

    Returns:
        dict: ``{"root_status": int or None, "robots_status": int or None,
        "reachable": bool}``
    """
    root_url = f"https://{domain}"
    robots_url = f"{root_url}/robots.txt"

    root_status = _fetch_status(root_url)
    robots_status = _fetch_status(robots_url)

    reachable = False
    for status in (root_status, robots_status):
        if status is not None and status < 500:
            reachable = True
            break

    return {
        "root_status": root_status,
        "robots_status": robots_status,
        "reachable": reachable,
    }
