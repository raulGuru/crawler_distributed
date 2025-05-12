#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Base settings shared across all components of the crawler system.
This module provides centralized configuration to maintain consistency.
"""

import os
import socket
import logging
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data storage directories
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
HTML_DIR = os.path.join(DATA_DIR, 'html')
LOG_DIR = os.path.join(DATA_DIR, 'logs')

# Create directories if they don't exist
for directory in [DATA_DIR, HTML_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Worker script paths
CRAWLER_JOB_LISTENER_PATH = os.path.join(PROJECT_ROOT, 'crawler', 'worker', 'crawl_job_listener.py')
PARSE_WORKER_PATH = os.path.join(PROJECT_ROOT, 'workers', 'job_dispatcher.py')
MONITOR_WORKER_PATH = os.path.join(PROJECT_ROOT, 'workers', 'monitor_worker.py')

# Scrapy
SCRAPY_PATH = 'scrapy'
DEFAULT_MAX_PAGES = 50
DEFAULT_SINGLE_URL = False
DEFAULT_USE_SITEMAP = False

# Maximum concurrent crawlers
MAX_CONCURRENT_CRAWLERS = 5

# Health check interval (seconds)
HEALTH_CHECK_INTERVAL = 60

# System monitoring thresholds
CPU_WARNING_THRESHOLD = 80  # percentage
MEMORY_WARNING_THRESHOLD = 80  # percentage
DISK_WARNING_THRESHOLD = 80  # percentage

# MongoDB configuration
MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_DB = os.environ.get('MONGO_DB', 'crawler_db')
MONGO_USER = os.environ.get('MONGO_USER', '')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD', '')
MONGO_AUTH_SOURCE = os.environ.get('MONGO_AUTH_SOURCE', 'admin')
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={MONGO_AUTH_SOURCE}" if MONGO_USER else f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
MONGO_CRAWL_JOB_COLLECTION = 'crawl_jobs'
MONGO_PARSE_JOB_COLLECTION = 'parse_jobs'

# Beanstalkd queue configuration
QUEUE_HOST = os.environ.get('QUEUE_HOST', 'localhost')
QUEUE_PORT = int(os.environ.get('QUEUE_PORT', '11300'))
QUEUE_TUBES = ['crawl_jobs', 'parse_jobs', 'monitor_jobs']
QUEUE_CRAWL_TUBE = os.environ.get('QUEUE_CRAWL_TUBE', 'crawl_jobs')
QUEUE_PARSE_TUBE = os.environ.get('QUEUE_PARSE_TUBE', 'parse_jobs')
QUEUE_MONITOR_TUBE = os.environ.get('QUEUE_MONITOR_TUBE', 'monitor_jobs')
QUEUE_TTR = int(os.environ.get('QUEUE_TTR', 900))

# General settings
HOSTNAME = socket.gethostname()