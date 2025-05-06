#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Proxy manager utility for handling proxy operations.
"""

import json
import logging
from typing import Optional, Dict, List
from pathlib import Path

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Manages proxy operations including loading, selection, and rotation.
    """

    def __init__(self, proxy_list_path: str):
        """Initialize the proxy manager."""
        self.proxy_list_path = proxy_list_path
        self.proxies: List[str] = []
        self.proxy_stats: Dict[str, Dict] = {}
        self._load_proxies()

    def _load_proxies(self):
        """Load proxies from the configuration file."""
        try:
            proxy_path = Path(self.proxy_list_path)
            if not proxy_path.exists():
                raise FileNotFoundError(f"Proxy list file not found: {self.proxy_list_path}")

            with open(proxy_path) as f:
                config = json.load(f)
                if not isinstance(config, dict) or 'proxies' not in config:
                    raise ValueError("Invalid proxy list format. Expected {'proxies': [...]}")

                self.proxies = config['proxies']
                if not self.proxies:
                    logger.warning("No proxies found in proxy list file")

            # Initialize stats for each proxy
            for proxy in self.proxies:
                if not isinstance(proxy, str):
                    logger.warning(f"Invalid proxy format: {proxy}, skipping")
                    continue
                self.proxy_stats[proxy] = {
                    'success': 0,
                    'failure': 0,
                    'banned': 0
                }

            logger.info(f"Loaded {len(self.proxies)} proxies from {self.proxy_list_path}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse proxy list JSON: {str(e)}")
            self.proxies = []
        except Exception as e:
            logger.error(f"Failed to load proxy list: {str(e)}")
            self.proxies = []

    def get_proxy(self) -> Optional[str]:
        """Get the best available proxy."""
        if not self.proxies:
            return None

        # Select proxy with best success rate
        return max(
            self.proxies,
            key=lambda p: (
                self.proxy_stats[p]['success'] + 1) /
                (self.proxy_stats[p]['failure'] + self.proxy_stats[p]['banned'] + 1
            )
        )

    def mark_success(self, proxy: str):
        """Mark a proxy as successful."""
        if proxy in self.proxy_stats:
            self.proxy_stats[proxy]['success'] += 1

    def mark_failure(self, proxy: str):
        """Mark a proxy as failed."""
        if proxy in self.proxy_stats:
            self.proxy_stats[proxy]['failure'] += 1

    def mark_banned(self, proxy: str):
        """Mark a proxy as banned."""
        if proxy in self.proxy_stats:
            self.proxy_stats[proxy]['banned'] += 1

    def get_stats(self) -> Dict[str, Dict]:
        """Get proxy statistics."""
        return self.proxy_stats

# Global proxy manager instance
_proxy_manager: Optional[ProxyManager] = None

def init_proxy_manager(proxy_list_path: str):
    """Initialize the global proxy manager."""
    global _proxy_manager
    _proxy_manager = ProxyManager(proxy_list_path)

def get_proxy_manager() -> ProxyManager:
    """Get the global proxy manager instance."""
    if _proxy_manager is None:
        raise RuntimeError("Proxy manager not initialized. Call init_proxy_manager first.")
    return _proxy_manager
