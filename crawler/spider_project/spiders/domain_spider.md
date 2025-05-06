# File Overview

- **Purpose:**
  Implements a Scrapy spider (`DomainSpider`) for crawling entire domains, supporting both sitemap-based and breadth-first search (BFS) crawling strategies.
- **Key Features:**
  - Can use sitemaps for URL discovery if available and enabled.
  - Defaults to BFS crawling for uniform coverage.
  - Handles queueing, deduplication, and limits on crawling.
  - Integrates with Scrapy’s request/response system.
  - Includes robust error handling and logging.

---

# Imports

- **Standard Library:**
  `logging`, `collections.deque`, `re`, `urllib.parse`, `typing`
- **Scrapy:**
  `Request`, `Response`, `CrawlSpider`, `Rule`, `Spider`, `LinkExtractor`, `CloseSpider`, `get_base_url`
- **Project/Local:**
  - `.base_spider.BaseSpider` (base class)
  - `lib.utils.sitemap_utils` (sitemap helpers)
  - `..utils.url_utils.has_skipped_extension` (extension filter)

---

# Class: `DomainSpider(BaseSpider)`

## Purpose

A spider for crawling all pages of a domain, with options for sitemap-based or BFS crawling. Designed for flexibility, efficiency, and robustness.

## Class Attributes

- `name`: `'domain_spider'` (Scrapy spider name)

---

## Class Methods

### `from_crawler(cls, crawler, *args, **kwargs)`

- **Purpose:**
  Factory method to create a spider instance with settings from Scrapy’s crawler.
- **Process:**
  - Reads spider-specific settings from Scrapy’s settings.
  - Sets attributes like queue size, batch size, retry/backoff parameters, sitemap age, concurrency, etc.

---

## Instance Methods

### `__init__(...)`

- **Purpose:**
  Initializes the spider with domain, crawl/job IDs, max pages, sitemap usage, and start URLs.
- **Key Attributes:**
  - `domain`: Target domain.
  - `use_sitemap`: Whether to use sitemap for URL discovery.
  - `url_queue`: Deque for managing URLs to crawl.
  - `currently_crawling`: Set of URLs currently being crawled.
  - `crawled_urls`: Set of already crawled URLs.
  - `unique_pages_crawled`: Counter for unique pages crawled.
  - `allowed_domains`: List of allowed domains (handles www/non-www).
  - `start_urls`: List of starting URLs (http/https).
  - `link_extractor`: Configured Scrapy `LinkExtractor` with allowed domains and deny rules for extensions and patterns.

---

### `_enqueue_url(url, callback=None, meta=None)`

- **Purpose:**
  Adds a URL to the crawl queue if it passes checks (not already queued/crawled, not skipped extension, queue not full, max pages not reached).
- **Checks:**
  - Max pages (soft check via Scrapy stats).
  - Skipped extensions (via `has_skipped_extension`).
  - Queue size.
  - Deduplication (not in queue or currently crawling).
- **Side Effects:**
  Updates stats for skipped pages.

---

### `_get_next_urls()`

- **Purpose:**
  Pops the next URL from the queue for crawling, ensuring it’s not already being crawled.
- **Returns:**
  URL data dict or `None`.

---

### `_remove_from_crawling(url)`

- **Purpose:**
  Removes a URL from the `currently_crawling` set, with error handling.

---

### `start_requests()`

- **Purpose:**
  Entry point for Scrapy spiders. Yields initial requests to start crawling.
- **Process:**
  1. Enqueues homepage URL.
  2. If `use_sitemap` is enabled, tries to locate and process sitemap.
  3. If sitemap is not used/found, starts BFS crawling from homepage.
- **Yields:**
  Scrapy `Request` objects.

---

### `_handle_homepage(response)`

- **Purpose:**
  Handles the homepage response, decides whether to proceed with sitemap or BFS crawling.
- **Process:**
  - Yields parsed homepage.
  - If sitemap is enabled and not processed, enqueues robots.txt for sitemap discovery.
  - If max pages not reached, starts BFS crawl.

---

### `_process_sitemap_urls(sitemap_url)`

- **Purpose:**
  Processes a sitemap URL (regular or index), extracts URLs, prioritizes, and enqueues them.
- **Process:**
  - Checks sitemap age.
  - Fetches sitemap content.
  - If sitemap index, recursively processes child sitemaps.
  - Prioritizes URLs (via `prioritize_urls`).
  - Enqueues and yields requests for URLs.
  - Marks sitemap as processed.

---

### `_parse_sitemap(response)`

- **Purpose:**
  Parses a sitemap XML response, generates requests for each URL.
- **Process:**
  - Uses `_process_sitemap_urls`.
  - If no URLs found, falls back to BFS crawl.

---

### `_parse_robots(response)`

- **Purpose:**
  Parses robots.txt to find sitemap locations.
- **Process:**
  - Looks for `Sitemap:` directives.
  - Processes found sitemaps.
  - If none found, tries common locations.
  - If still none, falls back to BFS crawl.

---

### `_handle_sitemap_error(failure)`

- **Purpose:**
  Handles errors in sitemap fetching by falling back to BFS crawl.

---

### `_start_bfs_crawl(response)`

- **Purpose:**
  Starts BFS crawling from a given response.
- **Process:**
  - Extracts links using `link_extractor`.
  - Enqueues links.
  - Yields requests for queued URLs, respecting max pages and concurrency.

---

### `_parse_page(response)`

- **Purpose:**
  Parses a crawled page, extracts and enqueues new links, yields page data.
- **Process:**
  - Removes URL from `currently_crawling`.
  - Adds to `crawled_urls`, increments counter.
  - Extracts and enqueues new links if under max pages.
  - Yields parsed page data (URL, status, HTML, headers, etc.).

---

### `_is_sitemap_too_old(lastmod)`

- **Purpose:**
  Checks if a sitemap’s last modification date is older than allowed.
- **Process:**
  Tries multiple date formats, compares with current date.

---

### `handle_error(failure)`

- **Purpose:**
  Handles failed requests.
- **Process:**
  - Removes URL from `currently_crawling`.
  - Calls parent error handler.
  - If a retry request is returned, re-enqueues it.
  - Processes next URL in queue.

---

# Key Processes and Flow

1. **Initialization:**
   Spider is configured with domain, settings, and crawling strategy (sitemap or BFS).

2. **Start Requests:**
   - Homepage is enqueued and requested.
   - If sitemap is enabled, attempts to discover and process sitemap.
   - If sitemap is not used/found, starts BFS crawl.

3. **Sitemap Processing:**
   - Locates sitemap via robots.txt or common locations.
   - Processes sitemap or sitemap index, extracts and prioritizes URLs.
   - Enqueues and yields requests for discovered URLs.

4. **BFS Crawling:**
   - Extracts links from each crawled page.
   - Enqueues new links, respecting deduplication and limits.
   - Yields requests for new URLs.

5. **Page Parsing:**
   - For each crawled page, yields structured data (URL, status, HTML, headers, etc.).
   - Continues crawling until max pages or queue is exhausted.

6. **Error Handling:**
   - Handles failed requests, retries if possible, and continues crawling.

---

# Logging and Stats

- **Logging:**
  Extensive use of logging for info, warnings, and errors, including context (URLs, counts).
- **Stats:**
  Tracks pages crawled, skipped, and other metrics via Scrapy’s stats system.

---

# Extension and Customization

- **Settings:**
  Many parameters (queue size, batch size, retries, concurrency, etc.) are configurable via Scrapy settings.
- **Link Extraction:**
  Deny rules for extensions and URL patterns are customizable.
- **Sitemap Handling:**
  Modular functions for sitemap discovery, parsing, and prioritization.

---

# PEP 8 and Project Conventions

- **Naming:**
  Uses snake_case for functions/variables, PascalCase for classes.
- **Docstrings:**
  Present for all major methods and classes.
- **Imports:**
  Grouped and organized.
- **Logging:**
  Follows severity levels and includes context.

---

# Summary Table

| Function/Method              | Purpose/Role                                                                 |
|------------------------------|------------------------------------------------------------------------------|
| `from_crawler`               | Instantiates spider with settings                                            |
| `__init__`                   | Initializes spider attributes and config                                     |
| `_enqueue_url`               | Adds URL to crawl queue with checks                                         |
| `_get_next_urls`             | Gets next URL to crawl from queue                                           |
| `_remove_from_crawling`      | Removes URL from currently crawling set                                     |
| `start_requests`             | Entry point, yields initial requests                                        |
| `_handle_homepage`           | Handles homepage response, decides crawl strategy                           |
| `_process_sitemap_urls`      | Processes sitemap or index, enqueues URLs                                   |
| `_parse_sitemap`             | Parses sitemap XML, yields requests                                         |
| `_parse_robots`              | Parses robots.txt for sitemap locations                                     |
| `_handle_sitemap_error`      | Handles sitemap fetch errors                                                |
| `_start_bfs_crawl`           | Starts BFS crawling from a response                                         |
| `_parse_page`                | Parses a crawled page, yields data, enqueues new links                      |
| `_is_sitemap_too_old`        | Checks if sitemap is outdated                                               |
| `handle_error`               | Handles failed requests, retries, continues crawl                           |

---

# Conclusion

This file implements a robust, configurable, and extensible domain spider for Scrapy, supporting both sitemap and BFS crawling. It is well-structured, follows PEP 8 and project conventions, and is designed for reliability and maintainability. All major crawling, queueing, deduplication, and error handling logic is encapsulated in clearly documented methods, making it easy to extend or adapt for new requirements.
