## Distributed Web Crawler

A scalable distributed web crawler system using Scrapy, Beanstalkd, and MongoDB.

### Features

- Distributed architecture with queue-based job distribution
- Domain crawls and single URL crawls
- Sitemap support for efficient URL discovery
- BFS (Breadth-First Search) crawling
- Proxy handling and rotation
- JavaScript rendering capabilities
- Duplicate URL detection and filtering
- Monitoring and error handling
- Environment-based default parameters

### System Components

- **Queue Listener**: Listens for new crawl jobs and dispatches them
- **Crawler Launcher**: Manages Scrapy spiders for crawling
- **Parse Worker**: Processes HTML content
- **Monitor Worker**: Monitors system health and detects stalled jobs
- **Integration Service**: Orchestrates all components with monitoring and error handling

### Prerequisites

- Python 3.8+
- MongoDB
- Beanstalkd
- Scrapy 2.12+
- (Optional) Splash or Playwright for JavaScript rendering

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/crawler_scrapy_distributed.git
   cd crawler_scrapy_distributed
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure MongoDB and Beanstalkd are running:
   ```bash
   # Start MongoDB (adjust as needed for your setup)
   mongod --dbpath /path/to/data/db

   # Start Beanstalkd
   beanstalkd
   ```

4. Set up default parameters (optional):
   Create a `.env` file in the project root with your default values:
   ```
   # Default crawler settings

   # Database
   DB_URI=mongodb://localhost:27017/crawler
   MONGO_HOST=localhost
   MONGO_PORT=27017
   MONGO_DB=crawler_db
   MONGO_USER=
   MONGO_PASSWORD=

   # Queue
   QUEUE_HOST=localhost
   QUEUE_PORT=11300

   # Crawler settings
   DEFAULT_MAX_PAGES=1000
   DEFAULT_SINGLE_URL=False
   DEFAULT_USE_SITEMAP=True
   DEFAULT_RATE_LIMIT=60

   # System settings
   MAX_CONCURRENT_CRAWLERS=5
   ```
   If no `.env` file exists, the system will create one with default values.

### Usage

#### Starting the System

The crawler system is designed to run as a set of coordinated services. The easiest way to start the entire system is using the integration service:

```bash
./scripts/run_integration.sh
```

This will start all necessary workers:
- Queue Listener (listens for crawl jobs)
- Parser Workers (process HTML content)
- Monitor Worker (monitors system health)

#### Configuration Options

You can customize the system behavior by passing options to the integration service:

```bash
./scripts/run_integration.sh --queue-host localhost --queue-port 11300 --db-uri mongodb://localhost:27017/crawler
```

Available options:
- `--queue-host`: Beanstalkd host (default: localhost)
- `--queue-port`: Beanstalkd port (default: 11300)
- `--db-uri`: MongoDB URI (default: mongodb://localhost:27017/crawler)
- `--health-check-interval`: Health check interval in seconds (default: 60)

#### Submitting Crawl Jobs

You can submit crawl jobs using the provided script:

```bash
# Submit a domain crawl job with all parameters specified
python scripts/submit_job.py --domain example.com --max-pages 500 --use-sitemap

# Submit a domain crawl job with minimal parameters (others use defaults from .env)
python scripts/submit_job.py --domain example.com

# Submit a single URL crawl
python scripts/submit_job.py --url https://example.com/page --single-url
```

Available parameters:
- `--domain`: Domain to crawl (required if URL not provided)
- `--url`: URL to crawl (required if domain not provided)
- `--max-pages`: Maximum number of pages to crawl (default: from .env or 1000)
- `--single-url`: Crawl a single URL only (default: from .env or False)
- `--use-sitemap`: Use sitemap for URL discovery (default: from .env or True)
- `--queue-host`: Beanstalkd host (default: from .env or localhost)
- `--queue-port`: Beanstalkd port (default: from .env or 11300)
- `--confirm`: Confirm before submitting the job

Or programmatically using the QueueManager:

```python
from lib.queue.queue_manager import QueueManager
from lib.storage.mongodb_client import MongoDBClient
from lib.storage.state_manager import StateManager
from datetime import datetime

# Initialize components
mongodb_client = MongoDBClient(uri='mongodb://localhost:27017/crawler')
state_manager = StateManager(mongodb_client)
queue_manager = QueueManager(host='localhost', port=11300)

# Create job data (only include parameters you want to override)
job_data = {
    'job_type': 'crawl',
    'domain': 'example.com',
    'submitted_at': datetime.utcnow().isoformat()
}

# Create job in database (will use defaults from .env for missing parameters)
crawl_id = state_manager.create_crawl_job(job_data)

# Submit to queue with only the crawl_id
queue_manager.enqueue_job(
    job_data={'crawl_id': crawl_id},
    tube='crawl_jobs'
)
```

#### Monitoring

The system includes built-in monitoring through the monitor worker and integration service. Health check reports are generated periodically and stored in the `data/logs` directory.

You can view the logs for each component:
- `data/logs/integration_service.log`
- `data/logs/queue_listener_0.log`
- `data/logs/monitor_worker_0.log`
- `data/logs/parse_worker_N.log` (where N is the worker instance ID)

To check the status of crawler jobs, use the job status script:

```bash
# List recent jobs
./scripts/job_status.py list

# View details of a specific job
./scripts/job_status.py get YOUR_CRAWL_ID

# List jobs with a specific status
./scripts/job_status.py status --status running
```

See `scripts/README.md` for more details on the job status script.

### Error Handling

The system includes robust error handling mechanisms:

1. **Component-level error handling**: Each component has its own error handling mechanisms to recover from failures
2. **Job retry**: Failed jobs are retried with exponential backoff
3. **Worker monitoring**: The integration service monitors worker processes and restarts them if they fail
4. **Health checks**: Periodic health checks detect issues with dependencies (MongoDB, Beanstalkd)
5. **Circuit breakers**: The system includes circuit breakers to prevent overwhelming failing components

### Architecture

The distributed crawler system uses a message queue architecture to decouple components:

1. Jobs are enqueued to Beanstalkd
2. Queue listener dispatches jobs to crawler processes
3. Crawlers fetch pages and store to disk
4. Parse jobs are enqueued for processing
5. Parse workers extract data from HTML files

This design allows for horizontal scaling and resilience to failures.

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### License

This project is licensed under the MIT License - see the LICENSE file for details.