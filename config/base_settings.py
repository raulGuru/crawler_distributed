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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data storage directories
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
HTML_DIR = os.path.join(DATA_DIR, 'html')
LOG_DIR = os.path.join(DATA_DIR, 'logs')
INTEGRATION_SERVICE_LOG_DIR = os.path.join(LOG_DIR, 'integration_service')
SUBMIT_CRAWL_JOBS_DIR = os.path.join(LOG_DIR, 'submit_crawl_jobs')
CRAWL_JOB_LISTENERS_DIR = os.path.join(LOG_DIR, 'crawl_job_listeners')
SCRAPY_LOGS_DIR = os.path.join(LOG_DIR, 'scrapy_logs')
PARSER_WORKERS_DIR = os.path.join(LOG_DIR, 'parser_workers')
HEALTH_CHECKS_DIR = os.path.join(LOG_DIR, 'health_checks')
DOMAIN_CONFIG_FILE = os.path.join(DATA_DIR, 'domain_config.json')

# Create directories if they don't exist
for directory in [DATA_DIR, HTML_DIR, LOG_DIR, INTEGRATION_SERVICE_LOG_DIR, SUBMIT_CRAWL_JOBS_DIR, PARSER_WORKERS_DIR, CRAWL_JOB_LISTENERS_DIR, HEALTH_CHECKS_DIR, SCRAPY_LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Worker script paths
CRAWLER_JOB_LISTENER_PATH = os.path.join(PROJECT_ROOT, 'crawler', 'worker', 'crawl_job_listener.py')
PARSE_WORKER_PATH = os.path.join(PROJECT_ROOT, 'workers', 'job_dispatcher.py')
MONITOR_WORKER_PATH = os.path.join(PROJECT_ROOT, 'workers', 'monitor_worker.py')
CRAWLER_INSTANCES = int(os.getenv("CRAWLER_INSTANCES", 2))

# Scrapy
SCRAPY_PATH = 'scrapy'
DEFAULT_MAX_PAGES = 25
DEFAULT_SINGLE_URL = False
DEFAULT_USE_SITEMAP = False

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
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
    f"?authSource={MONGO_AUTH_SOURCE}"
    # f"&authMechanism=SCRAM-SHA-1"
    if MONGO_USER else
    f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
)
MONGO_CRAWL_JOB_COLLECTION = os.environ.get('MONGO_CRAWL_JOB_COLLECTION', 'crawl_jobs')
MONGO_PARSED_HTML_COLLECTION = os.environ.get('MONGO_PARSED_HTML_COLLECTION', 'parsed_html_data')

# Beanstalkd queue configuration
QUEUE_HOST = os.environ.get('QUEUE_HOST', 'localhost')
QUEUE_PORT = int(os.environ.get('QUEUE_PORT', 11300))
QUEUE_CRAWL_TUBE = os.environ.get('QUEUE_CRAWL_TUBE', 'crawler_crawl_jobs')
QUEUE_TTR = int(os.environ.get('QUEUE_TTR', 300))

# Core workers
CORE_WORKERS = {
        'crawl_job_listener': {
            'script': CRAWLER_JOB_LISTENER_PATH,
            'required': True,  # System requires this worker
            'instances': CRAWLER_INSTANCES,    # Number of instances to run
            'restart': True,   # Auto-restart if it crashes
            'args': []         # Additional command line arguments
        }
    }

# General settings
HOSTNAME = socket.gethostname()