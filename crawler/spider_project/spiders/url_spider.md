# URLSpider

A Scrapy spider for crawling single URLs without following links.

## Overview

`URLSpider` is designed to crawl a single URL, extract its contents, and store the HTML. Unlike the `DomainSpider`, it does not traverse links or attempt to crawl beyond the target URL. It uses the same error handling and fallback mechanisms as other spiders in the system.

## Key Features

- Makes a single request to the specified URL
- Does not follow links (depth=0)
- Implements graceful error handling with proxy/JS rendering fallbacks
- Integrates with Scrapy's request/response system
- Stores HTML content via pipelines

## Usage

```python
# Command line
scrapy crawl url_spider -a url="https://example.com" -a job_id="my_job_123"

# From Python code
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from crawler.spider_project.spiders.url_spider import URLSpider

process = CrawlerProcess(get_project_settings())
process.crawl(URLSpider, url="https://example.com", job_id="my_job_123")
process.start()
```

## Parameters

- `url` (required): The target URL to crawl
- `job_id` or `crawl_id` (required): Unique identifier for this crawl
- `max_pages` (optional): Maximum number of pages to crawl (defaults to 1, ignored since URLSpider only crawls one page)

## Settings

Key settings for URLSpider (from `settings.py`):

```python
URL_SPIDER_SETTINGS = {
    'DEPTH_LIMIT': 0,  # Don't follow any links
    'DOWNLOAD_DELAY': 0,  # No delay needed for single URL
    'CONCURRENT_REQUESTS_PER_DOMAIN': 1,  # Only need one request
    'DOWNLOAD_TIMEOUT': 45,  # Longer timeout for single URL
    'RETRY_ENABLED': True,
    'RETRY_TIMES': 3,  # More retries for single URL
    'REDIRECT_ENABLED': True,  # Follow redirects
    'REDIRECT_MAX_TIMES': 10,  # Allow more redirects for single URL
    # ... additional settings
}
```

## Process Flow

1. **Initialization:**
   Spider is configured with target URL and job parameters.

2. **Start Request:**
   A single request is made to the target URL.

3. **Error Handling:**
   If direct request fails, the system tries with a proxy.
   If proxy request fails, the system tries with JS rendering.

4. **Page Parsing:**
   The HTML is extracted and yielded to the pipeline system.

5. **Completion:**
   Spider is closed after processing the single URL.

## Error Handling

The spider implements progressive fallback strategies when URL fetching fails:

1. Direct crawl (no proxy, no JS rendering)
2. Proxy crawl (if direct fails)
3. JS rendering (if proxy fails)

This ensures maximum coverage even for URLs that require special handling.

## Related Files

- `url_spider.py`: Main implementation
- `base_spider.py`: Parent class with common functionality
- `examples/single_url_crawl.py`: Example usage