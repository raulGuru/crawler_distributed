# Distributed Web Crawler (Scrapy + Beanstalkd + MongoDB)

A scalable, modular, distributed web crawler system. Handles large-scale domain and single-URL crawls, with robust parsing, queueing, monitoring, and JavaScript rendering.

---

## Features
- Distributed, horizontally scalable architecture
- Queue-based job distribution (Beanstalkd)
- Domain and single-URL crawls
- Sitemap and BFS crawling
- Proxy rotation and management
- JavaScript rendering (Playwright, Splash, Selenium, Puppeteer)
- Duplicate URL detection
- Monitoring, health checks, auto-restart
- Extensible parser worker system
- Docker Compose support for full stack

---

## Architecture & Flow

1. **Job Submission**: Submit crawl jobs via script/API → Beanstalkd queue
2. **Queue Listener**: Picks up jobs, launches Scrapy spiders
3. **Crawler**: Crawls pages, stores HTML, enqueues parse jobs
4. **Parser Workers**: Specialized workers extract data from HTML (see `parser/workers/`)
5. **Monitor Worker**: Monitors queue, system health, logs metrics
6. **Integration Service**: Orchestrates all workers, restarts on failure, health checks
7. **Health-Check Step**: Periodic system checks are executed by the integration
   service. Results are saved to `data/logs/health_checks` for troubleshooting.

**Key Components:**
- `workers/integration_service.py`: Orchestrates all workers, health checks, restarts
- `workers/worker_manager.py`: Starts/stops/monitors worker processes
- `workers/monitor_worker.py`: Monitors queue and system health
- `parser/workers/`: Specialized HTML/data extractors (JS, AMP, links, images, etc)
- `lib/queue/`: QueueManager, Beanstalkd client, job serialization
- `lib/storage/`: MongoDB client, file storage, state manager
- `lib/renderers/`: Playwright, Splash, Selenium, Puppeteer support
- `lib/utils/`: Logging, health checks, URL/proxy/sitemap utilities
- `crawler/`: Scrapy project, spiders, pipelines, middlewares
- `scripts/`: Job submission, status, setup, integration runner
- `config/`: All settings, logging, proxies
- `data/`: HTML and logs

---

## Quickstart

### Prerequisites
- Python 3.8+
- MongoDB
- Beanstalkd
- Scrapy 2.12+
- (Optional) Splash, Playwright, Selenium, Puppeteer for JS rendering

### Installation
```bash
git clone https://github.com/yourusername/crawler_scrapy_distributed.git
cd crawler_scrapy_distributed
pip install -r requirements.txt
cp env.example .env  # Edit as needed
```

### Running the Full Stack (Recommended)
```bash
docker-compose up --build
```
This launches MongoDB, Beanstalkd, Splash, and all worker services. Logs are in `data/logs/`.

### Manual/Dev Run
```bash
./scripts/run_integration.sh
```
This starts all workers locally (Queue Listener, Crawler, Parser, Monitor).

---

## Usage

### Submitting Crawl Jobs
```bash
python scripts/submit_crawl_job.py --domain example.com --max-pages 500 --use-sitemap
python scripts/submit_crawl_job.py --url https://example.com/page --single-url
```
See `python scripts/submit_crawl_job.py --help` for all options.

### Monitoring & Status
```bash
python scripts/job_status.py list
python scripts/job_status.py status --status running
python scripts/job_status.py get <CRAWL_ID>
```
Logs: `data/logs/`
Job status script details: `scripts/README.md`

### Configuration
- `.env`: Main config (copied from `env.example`)
- `config/`: Python config files (settings, logging, proxies)

### Domain Configuration
Per-domain settings such as `use_proxy` and `use_js_rendering` are stored in
MongoDB. When a new job is created for a domain, these flags are applied so
subsequent crawls continue with the learned strategy. Use the job submission
script to override them if needed:

```bash
python scripts/submit_crawl_job.py --domain example.com --use-proxy --use-js-rendering
```
This overrides any stored configuration for that run.

### Retry Strategy
Each request is attempted up to three times. The spider escalates through these
stages:

1. Direct fetch without extras
2. Retry with a proxy
3. Retry with proxy and JavaScript rendering

If all strategies fail the job is marked failed and may be buried by the queue
manager.

---

## Extending the System
- **Add new parser workers**: Drop a new worker in `parser/workers/` (see existing ones for template)
- **Add new spiders**: Add to `crawler/spider_project/spiders/`
- **Add pipelines/middlewares**: `crawler/spider_project/pipelines/`, `crawler/spider_project/middlewares/`
- **Custom queue logic**: `lib/queue/`
- **Custom storage**: `lib/storage/`

---

## Project Structure (Key Parts)

```
config/           # All config (settings, logging, proxies)
crawler/          # Scrapy project, spiders, pipelines, middlewares
parser/workers/   # Specialized parser workers (JS, AMP, links, etc)
lib/queue/        # QueueManager, Beanstalkd client, job serialization
lib/storage/      # MongoDB client, file storage, state manager
lib/renderers/    # Playwright, Splash, Selenium, Puppeteer
lib/utils/        # Logging, health checks, URL/proxy/sitemap utils
workers/          # Integration service, worker manager, monitor
scripts/          # Job submission, status, setup, integration runner
data/             # HTML and logs
docker-compose.yml# Full stack orchestration
```

---

## Troubleshooting
- **Logs**: `data/logs/`
- **Health**: Monitor worker, integration service
- **Docker**: `docker-compose logs`
- **Mongo/Beanstalkd**: Check containers or local services

---

## Contributing
- PRs welcome. Follow PEP8 and see `.clinerules` for style.

---

## License
MIT. See LICENSE file.