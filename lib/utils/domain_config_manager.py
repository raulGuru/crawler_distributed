import json
import logging
from datetime import datetime
from pathlib import Path

from config.base_settings import DOMAIN_CONFIG_FILE

logger = logging.getLogger(__name__)


def update_domain_config(domain: str, use_proxy: bool = False, use_js_rendering: bool = False) -> None:
    """Persist domain strategy configuration to disk."""
    if not domain:
        logger.error("Domain not provided for update_domain_config")
        return

    try:
        config = {}
        config_path = Path(DOMAIN_CONFIG_FILE)
        if config_path.exists():
            with open(config_path, 'r') as f:
                try:
                    config = json.load(f) or {}
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in DOMAIN_CONFIG_FILE, starting fresh")
                    config = {}

        if not isinstance(config, dict):
            config = {}

        config[str(domain)] = {
            'use_proxy': bool(use_proxy),
            'use_js_rendering': bool(use_js_rendering),
            'updated_at': datetime.utcnow().isoformat()
        }

        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Updated domain config for {domain}: proxy={use_proxy}, js_rendering={use_js_rendering}")
    except Exception as e:
        logger.error(f"Failed to update domain config for {domain}: {e}")

