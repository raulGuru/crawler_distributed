#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configures logging system for all components.
This module provides consistent logging across the entire application.
"""

import os
import sys
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

from .base_settings import LOG_DIR, LOG_LEVEL, HOSTNAME

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Define log file naming format
def get_log_filename(component_name):
    """Generate log filename based on component name and current date."""
    date_str = datetime.now().strftime('%Y-%m-%d')
    return os.path.join(LOG_DIR, f"{component_name}_{date_str}.log")

# Define formatters
DETAILED_FORMATTER = logging.Formatter(
    '%(asctime)s [%(name)s] [%(levelname)s] [%(process)d] [%(thread)d] - %(message)s'
)

CONSOLE_FORMATTER = logging.Formatter(
    '%(asctime)s [%(name)s] [%(levelname)s] - %(message)s'
)

# Log levels map
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}

def configure_logger(component_name, log_level=None):
    """
    Configure logger for a specific component.

    Args:
        component_name: Name of the component (used for log file naming)
        log_level: Optional override for the log level

    Returns:
        Configured logger instance
    """
    # Use provided log level or default from settings
    log_level = log_level or LOG_LEVEL
    numeric_level = LOG_LEVELS.get(log_level.upper(), logging.INFO)

    # Create logger
    logger = logging.getLogger(component_name)
    logger.setLevel(numeric_level)

    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # File handler (detailed logs)
    log_file = get_log_filename(component_name)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10485760,  # 10MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setFormatter(DETAILED_FORMATTER)
    file_handler.setLevel(numeric_level)
    logger.addHandler(file_handler)

    # Console handler (less detailed)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CONSOLE_FORMATTER)
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)

    # Add hostname and PID to log context
    logger = logging.LoggerAdapter(logger, {'hostname': HOSTNAME, 'pid': os.getpid()})

    return logger

# Configure root logger
def configure_root_logger():
    """Configure the root logger with basic settings."""
    numeric_level = LOG_LEVELS.get(LOG_LEVEL.upper(), logging.INFO)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Clear any existing handlers
    if root_logger.handlers:
        root_logger.handlers.clear()

    # Console handler for root logger
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CONSOLE_FORMATTER)
    root_logger.addHandler(console_handler)

    return root_logger

# Default loggers
crawler_logger = configure_logger('crawler')
queue_logger = configure_logger('queue')
worker_logger = configure_logger('worker')
storage_logger = configure_logger('storage')
render_logger = configure_logger('renderer')
root_logger = configure_root_logger()

# Log uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    """Log uncaught exceptions instead of printing them."""
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default exception handler for KeyboardInterrupt
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    root_logger.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback)
    )

# Set the exception handler
sys.excepthook = handle_exception

# Context manager for logging operation status
class LogOperation:
    """Context manager for logging the status of operations."""

    def __init__(self, logger, operation_name, **context):
        self.logger = logger
        self.operation_name = operation_name
        self.context = context
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        context_str = ' '.join(f"{k}={v}" for k, v in self.context.items())
        self.logger.info(f"Starting {self.operation_name} {context_str}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        ms = int(duration.total_seconds() * 1000)

        if exc_type:
            self.logger.error(
                f"Failed {self.operation_name} after {ms}ms: {exc_val}",
                exc_info=(exc_type, exc_val, exc_tb)
            )
        else:
            self.logger.info(f"Completed {self.operation_name} in {ms}ms")

        return False  # Don't suppress exceptions