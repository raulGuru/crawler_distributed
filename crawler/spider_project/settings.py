#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Centralized settings file for the crawler system.
Contains crawler-specific settings.
"""

import os
import random
from pathlib import Path
from config.base_settings import *

HOSTNAME = socket.gethostname()
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Data and logging directories
DATA_DIR = PROJECT_ROOT / 'data'
HTML_DIR = DATA_DIR / 'html'
LOG_DIR = DATA_DIR / 'logs'

# Ensure directories exist
for directory in [DATA_DIR, HTML_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Crawler identification
CRAWLER_ID = f"{HOSTNAME}-{os.getpid()}"
BOT_NAME = 'crawler'
SPIDER_MODULES = ['crawler.spider_project.spiders']
NEWSPIDER_MODULE = 'crawler.spider_project.spiders'

# User agent configuration
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
]
DEFAULT_USER_AGENT = random.choice(USER_AGENTS)
ROTATE_USER_AGENT = True
USER_AGENT_ROTATION_POLICY = 'per_domain'  # 'per_domain', 'per_request', or 'per_crawl'

# Concurrency settings
DEFAULT_CONCURRENT_REQUESTS = 1
DEFAULT_CONCURRENT_REQUESTS_PER_DOMAIN = 1
DEFAULT_CONCURRENT_REQUESTS_PER_IP = 0  # unlimited
CONCURRENT_REQUESTS = DEFAULT_CONCURRENT_REQUESTS
CONCURRENT_REQUESTS_PER_DOMAIN = DEFAULT_CONCURRENT_REQUESTS_PER_DOMAIN
CONCURRENT_REQUESTS_PER_IP = DEFAULT_CONCURRENT_REQUESTS_PER_IP

# Request throttling
# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 5.0
# AUTOTHROTTLE_MAX_DELAY = 60.0
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# AUTOTHROTTLE_DEBUG = False
DOWNLOAD_DELAY = 2 # 2 seconds between requests
DOWNLOAD_TIMEOUT = 30
# RANDOMIZE_DOWNLOAD_DELAY = True

# Robots.txt handling
ROBOTSTXT_OBEY = False
ROBOTSTXT_USER_AGENT = 'DistributedCrawler'

# Proxy configuration
PROXY_ENABLED = True
PROXY_MODE = 'rotate'  # 'rotate', 'sticky_domain', 'sticky_session'
PROXY_ROTATION_POLICY = 'round_robin'  # 'round_robin', 'least_used', 'performance_based'
PROXY_BLACKLIST_POLICY = 'timeout_5xx'  # 'timeout', '5xx', 'timeout_5xx', 'none'
PROXY_BLACKLIST_TIME = 1800  # seconds (30 minutes)
PROXY_LIST_PATH = str(PROJECT_ROOT / 'config/proxy_list.json')

# Crawler behavior settings
DEPTH_PRIORITY = 1
DOMAIN_SPIDER_DEPTH_LIMIT = 10
CLOSESPIDER_TIMEOUT = 900
MAX_FAILED_PAGES = 20

# Domain spider specific settings
DOMAIN_SPIDER_CONCURRENT_REQUESTS = 1
DOMAIN_SPIDER_CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOMAIN_SPIDER_DOWNLOAD_DELAY = 0.5
DOMAIN_SPIDER_QUEUE_SIZE = 1000
DOMAIN_SPIDER_BATCH_SIZE = 10
DOMAIN_SPIDER_MAX_RETRIES = 3
DOMAIN_SPIDER_RETRY_DELAY = 5
DOMAIN_SPIDER_BACKOFF_FACTOR = 2

# Job parameter settings
MAX_PAGES = DEFAULT_MAX_PAGES
SITEMAP_MAX_AGE_DAYS = 90
SITEMAP_MAX_AGE = SITEMAP_MAX_AGE_DAYS * 24 * 60 * 60

# Logging configuration
LOG_ENABLED = True
LOG_LEVEL = 'INFO'
LOG_FILE = str(LOG_DIR / 'crawler.log')
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Memory and debugging
MEMDEBUG_ENABLED = True
MEMUSAGE_LIMIT_MB = 4096  # 4GB memory limit
MEMUSAGE_NOTIFY_MAIL = ['admin@example.com']

# Telnet console
TELNETCONSOLE_ENABLED = True
TELNETCONSOLE_PORT = [6023, 6073]
TELNETCONSOLE_HOST = '127.0.0.1'

# Additional settings
COOKIES_ENABLED = True
# COOKIES_DEBUG = False
# REDIRECT_ENABLED = True
# REDIRECT_MAX_TIMES = 5
# AJAXCRAWL_ENABLED = True
# COMPRESSION_ENABLED = True
# REACTOR_THREADPOOL_MAXSIZE = 20
# DNSCACHE_ENABLED = True
# DNS_TIMEOUT = 10

# Storage settings
# HTML_STORAGE_ENABLED = True
# HTML_STORAGE_COMPRESS = True
HTML_STORAGE_FOLDER = str(HTML_DIR)
STATS_DUMP = True
STATS_DUMP_INTERVAL = 60

# URL filtering
# URL_ALLOW_DOMAINS = []
# URL_ALLOW_PATTERNS = []
# URL_DENY_PATTERNS = []

# Retry settings
RETRY_DELAY = 5  # seconds
RETRY_BACKOFF_FACTOR = 2  # exponential backoff

# Environment-specific settings
if os.environ.get('CRAWLER_ENV') == 'production':
    LOG_LEVEL = 'WARNING'
    # AUTOTHROTTLE_DEBUG = False
    CONCURRENT_REQUESTS = 1
    CONCURRENT_REQUESTS_PER_DOMAIN = 1
    MAX_PAGES = 50
    DOMAIN_SPIDER_CONCURRENT_REQUESTS = DOMAIN_SPIDER_CONCURRENT_REQUESTS
    DOMAIN_SPIDER_CONCURRENT_REQUESTS_PER_DOMAIN = DOMAIN_SPIDER_CONCURRENT_REQUESTS_PER_DOMAIN
    DOMAIN_SPIDER_QUEUE_SIZE = DOMAIN_SPIDER_QUEUE_SIZE
elif os.environ.get('CRAWLER_ENV') == 'development':
    LOG_LEVEL = 'DEBUG'
    # AUTOTHROTTLE_DEBUG = True
    CONCURRENT_REQUESTS = 1
    CONCURRENT_REQUESTS_PER_DOMAIN = 1
    MAX_PAGES = 50
    DOMAIN_SPIDER_CONCURRENT_REQUESTS = DOMAIN_SPIDER_CONCURRENT_REQUESTS
    DOMAIN_SPIDER_CONCURRENT_REQUESTS_PER_DOMAIN = DOMAIN_SPIDER_CONCURRENT_REQUESTS_PER_DOMAIN
    DOMAIN_SPIDER_QUEUE_SIZE = DOMAIN_SPIDER_QUEUE_SIZE

# Content types to process
ALLOWED_CONTENT_TYPES = [
    'text/html',
    'application/xhtml+xml',
    'application/xml',
    'text/xml',
]

# File extensions to skip
SKIPPED_EXTENSIONS = [
    # images
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico',
    # documents
    '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
    # archives
    '.zip', '.rar', '.gz', '.tar', '.7z',
    # audio/video
    '.mp3', '.mp4', '.avi', '.mov', '.flv', '.wmv',
    # other
    '.css', '.js', '.xml', '.json', '.csv', '.rss', '.atom',
]

# Middleware configuration (consolidated)
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy.downloadermiddlewares.offsite.OffsiteMiddleware': 500,
    'crawler.spider_project.middlewares.retry_middleware.RetryMiddleware': 550,
    'crawler.spider_project.middlewares.proxy_middleware.ProxyMiddleware': 560,
    'crawler.spider_project.middlewares.user_agent_middleware.UserAgentMiddleware': 570,
    'crawler.spider_project.middlewares.content_filter_middleware.ContentFilterMiddleware': 580,
    'crawler.spider_project.middlewares.js_rendering_middleware.JSRenderingMiddleware': 590,
    'crawler.spider_project.middlewares.stats_middleware.StatsMiddleware': 600,
    # 'crawler.spider_project.middlewares.max_pages_middleware.MaxPagesMiddleware': 610,
}

# Pipeline configuration (consolidated)
ITEM_PIPELINES = {
    'crawler.spider_project.pipelines.html_storage_pipeline.HTMLStoragePipeline': 100,
    'crawler.spider_project.pipelines.stats_pipeline.StatsPipeline': 200,
    'crawler.spider_project.pipelines.queue_pipeline.QueuePipeline': 300,
}

SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.depth.DepthMiddleware': 100,
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 200,
    'scrapy.spidermiddlewares.referer.RefererMiddleware': 400,
    'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': 500,
}

# JavaScript rendering configuration
JS_RENDERING_ENABLED = True  # Enable JS rendering by default
JS_RENDER_TIMEOUT = 40
DEFAULT_JS_RENDERER = 'playwright'  # Use Playwright as default

# Configure which domains are known to require JS rendering
JS_DOMAINS_FILE = str(DATA_DIR / 'js_domains.json')

# Configure JS rendering detection thresholds
JS_DETECTION_MIN_SCRIPTS = 5  # Minimum number of script tags to trigger detection
JS_DETECTION_CONTENT_RATIO = 0.5  # Script content to total content ratio threshold
JS_DETECTION_EMPTY_BODY_SIZE = 200  # Maximum size of "empty" body

# Configure JS rendering error handling
JS_RENDER_MAX_RETRIES = 3  # Maximum number of retries for JS rendering
JS_RENDER_RETRY_DELAY = 5  # Delay between retries in seconds
JS_RENDER_ERROR_THRESHOLD = 3  # Number of errors before disabling JS for a domain

# Configure JS rendering performance monitoring
JS_RENDER_PERFORMANCE_THRESHOLD = 10.0  # Seconds, warn if rendering takes longer
JS_RENDER_STATS_ENABLED = True  # Enable detailed rendering statistics

# Playwright settings
PLAYWRIGHT_BROWSER_TYPE = 'chromium'
PLAYWRIGHT_LAUNCH_OPTIONS = {
    'headless': True,
    'args': [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        '--window-size=1920,1080',
    ],
    'timeout': JS_RENDER_TIMEOUT * 1000,  # milliseconds
}
PLAYWRIGHT_DEFAULT_VIEWPORT = {'width': 1920, 'height': 1080}
PLAYWRIGHT_NAVIGATION_TIMEOUT = JS_RENDER_TIMEOUT * 1000  # milliseconds

# Remove Splash-specific settings since we're using Playwright
SPLASH_URL = None
SPLASH_WAIT = None
SPLASH_TIMEOUT = None
SPLASH_JS_SOURCE = None
DUPEFILTER_CLASS = 'scrapy.dupefilters.RFPDupeFilter'  # Use default Scrapy dupefilter
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'  # Use default Scrapy cache storage
HTTPCACHE_ENABLED = False

# Base spider settings
BASE_SPIDER_SETTINGS = {
    'MAX_FAILED_PAGES': MAX_FAILED_PAGES,
    'ROBOTSTXT_OBEY': ROBOTSTXT_OBEY,
    'COOKIES_ENABLED': COOKIES_ENABLED,
    'CONCURRENT_REQUESTS': CONCURRENT_REQUESTS,
    'DOWNLOAD_TIMEOUT': DOWNLOAD_TIMEOUT,
    'RETRY_ENABLED': True,
    'RETRY_TIMES': 3,
    'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
    'SITEMAP_MAX_AGE_DAYS': SITEMAP_MAX_AGE_DAYS,
}

# Domain spider settings
DOMAIN_SPIDER_SETTINGS = {
    **BASE_SPIDER_SETTINGS,
    'MAX_PAGES': MAX_PAGES,
    'DOWNLOAD_DELAY': DOWNLOAD_DELAY,
    'DEPTH_PRIORITY': DEPTH_PRIORITY,
    'QUEUE_SIZE': DOMAIN_SPIDER_QUEUE_SIZE,
    'BATCH_SIZE': DOMAIN_SPIDER_BATCH_SIZE,
    'DEPTH_LIMIT': DOMAIN_SPIDER_DEPTH_LIMIT,
    'MAX_RETRIES': DOMAIN_SPIDER_MAX_RETRIES,
    'RETRY_DELAY': DOMAIN_SPIDER_RETRY_DELAY,
    'BACKOFF_FACTOR': DOMAIN_SPIDER_BACKOFF_FACTOR,
    'CONCURRENT_REQUESTS_PER_DOMAIN': CONCURRENT_REQUESTS_PER_DOMAIN,
    'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.ScrapyPriorityQueue',
}

# URL spider settings
URL_SPIDER_SETTINGS = {
    **BASE_SPIDER_SETTINGS,
    'DEPTH_LIMIT': 0,
    'DOWNLOAD_DELAY': 0,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    'MAX_PAGES': 1,
}

# Spider-specific settings mapping
SPIDER_SPECIFIC_SETTINGS = {
    'domain_spider': DOMAIN_SPIDER_SETTINGS,
    'url_spider': URL_SPIDER_SETTINGS,
}