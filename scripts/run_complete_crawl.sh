#!/bin/bash

# Complete crawl script that manages the entire process from start to finish
# This script:
# 1. Cleans any existing data
# 2. Starts the integration service
# 3. Submits a crawl job
# 4. Monitors the job until completion
# 5. Shuts down all services when done

# Get the project root directory
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$PROJECT_ROOT"

# Activate virtual environment
source .venv/bin/activate

# Parse command line arguments
DOMAIN="befittingyoumedsupply.com"
URL=""
MAX_PAGES=15
USE_SITEMAP=false
CLEAN_DATA=true
TIMEOUT=300  # 5 minutes timeout
SINGLE_URL=false

print_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --domain DOMAIN        Domain to crawl (default: example.com)"
    echo "  --url URL              Specific URL to crawl (for single URL mode)"
    echo "  --max-pages NUM        Maximum pages to crawl (default: 10)"
    echo "  --no-sitemap           Disable sitemap usage"
    echo "  --use-sitemap          Enable sitemap usage"
    echo "  --clean                Clean all data before starting"
    echo "  --timeout SECONDS      Maximum time to wait for job completion (default: 300)"
    echo "  --single-url           Enable single URL mode (required when using --url)"
    echo "  --help                 Show this help message"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --domain)
            DOMAIN="$2"
            shift 2
            ;;
        --url)
            URL="$2"
            shift 2
            ;;
        --max-pages)
            MAX_PAGES="$2"
            shift 2
            ;;
        --no-sitemap)
            USE_SITEMAP=false
            shift
            ;;
        --use-sitemap)
            USE_SITEMAP=true
            shift
            ;;
        --clean)
            CLEAN_DATA=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --single-url)
            SINGLE_URL=true
            shift
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate parameters
if [ -z "$DOMAIN" ] && [ -z "$URL" ]; then
    echo "ERROR: Either --domain or --url must be specified"
    print_usage
    exit 1
fi

# If URL is provided without domain, extract domain from URL
if [ -z "$DOMAIN" ] && [ -n "$URL" ]; then
    # Basic domain extraction from URL using sed
    DOMAIN=$(echo "$URL" | sed -e 's|^[^/]*//||' -e 's|/.*$||' -e 's|^www\.||')
    echo "Extracted domain from URL: $DOMAIN"
fi

# If URL is provided, enable single_url mode automatically
if [ -n "$URL" ] && [ "$SINGLE_URL" = false ]; then
    echo "URL parameter provided - setting single_url=true"
    SINGLE_URL=true
fi

echo "===== Starting Complete Crawl Process ====="
echo "Domain:     $DOMAIN"
if [ -n "$URL" ]; then
    echo "URL:        $URL"
fi
echo "Max Pages:  $MAX_PAGES"
echo "Use Sitemap: $USE_SITEMAP"
echo "Single URL: $SINGLE_URL"
echo "Clean Data: $CLEAN_DATA"
echo "Timeout:    $TIMEOUT seconds"
echo "=========================================="

# Make sure no python processes are running
echo "Stopping any existing Python processes..."
killall -9 python 2>/dev/null || true
sleep 2

# Clean data if requested
if [ "$CLEAN_DATA" = true ]; then
    echo "Cleaning all data..."
    python clear_data.py
fi

# Start integration service in the background
echo "Starting integration service..."
./scripts/run_integration.sh > data/logs/integration_service_main.log 2>&1 &
INTEGRATION_PID=$!

# Give the integration service time to start
echo "Waiting for integration service to initialize..."
sleep 10

# Check if integration service is running
if ! ps -p $INTEGRATION_PID > /dev/null; then
    echo "ERROR: Integration service failed to start"
    echo "Check logs at data/logs/integration_service_main.log"
    exit 1
fi

# Submit the crawl job
echo "Submitting crawl job for $DOMAIN..."
SITEMAP_FLAG=""
if [ "$USE_SITEMAP" = true ]; then
    SITEMAP_FLAG="--use-sitemap"
else
    SITEMAP_FLAG="--no-sitemap"
fi

SINGLE_URL_FLAG=""
if [ "$SINGLE_URL" = true ]; then
    SINGLE_URL_FLAG="--single-url"
fi

URL_PARAM=""
if [ -n "$URL" ]; then
    URL_PARAM="--url $URL"
fi

# Build and execute the submit job command
SUBMIT_CMD="python scripts/submit_crawl_job.py --domain \"$DOMAIN\" --max-pages \"$MAX_PAGES\" $SITEMAP_FLAG $SINGLE_URL_FLAG $URL_PARAM"
echo "Executing: $SUBMIT_CMD"
JOB_OUTPUT=$(eval $SUBMIT_CMD)
echo "$JOB_OUTPUT"

# Extract the job ID
JOB_ID=$(echo "$JOB_OUTPUT" | grep "Job submitted successfully with ID" | sed 's/.*ID: \([^ ]*\).*/\1/')

if [ -z "$JOB_ID" ]; then
    echo "ERROR: Failed to extract job ID from output"
    echo "Shutting down integration service..."
    kill -9 $INTEGRATION_PID
    exit 1
fi

echo "Job submitted with ID: $JOB_ID"

# Monitor the job status
echo "Monitoring job $JOB_ID until completion..."
start_time=$(date +%s)
completed=false

while [ $(( $(date +%s) - start_time )) -lt $TIMEOUT ]; do
    # Wait a bit before checking
    sleep 5

    # Check job status
    STATUS_OUTPUT=$(python scripts/job_status.py list)
    echo "Current status:"
    echo "$STATUS_OUTPUT"

    # Check for completed status
    if echo "$STATUS_OUTPUT" | grep -q "completed"; then
        echo "Job completed successfully!"
        completed=true
        break
    fi

    # Check if there are any HTML files
    if [ -d "data/html/$DOMAIN" ] && [ "$(ls -A "data/html/$DOMAIN")" ]; then
        FOUND_FILES=$(ls -A "data/html/$DOMAIN" | wc -l)
        echo "Found $FOUND_FILES HTML files in data/html/$DOMAIN"
    fi
done

# Verify results
if [ "$completed" = true ]; then
    echo "===== Crawl Results ====="
    if [ -d "data/html/$DOMAIN" ]; then
        echo "HTML Files:"
        ls -la "data/html/$DOMAIN"

        # For single URL mode, check for the specific URL file
        if [ "$SINGLE_URL" = true ] && [ -n "$URL" ]; then
            # Extract path from URL to look for the file
            URL_PATH=$(echo "$URL" | sed -e 's|^[^/]*//[^/]*/||' -e 's|/$||')
            if [ -n "$URL_PATH" ]; then
                URL_FILE=$(find "data/html/$DOMAIN" -type f -name "*${URL_PATH}*" | head -1)
                if [ -n "$URL_FILE" ]; then
                    echo "Found specific URL file: $URL_FILE"
                    echo "Content preview:"
                    head -10 "$URL_FILE"
                else
                    echo "Specific URL file not found for: $URL_PATH"
                fi
            fi
        fi
    else
        echo "No HTML files found in data/html/$DOMAIN"
    fi
else
    echo "WARNING: Job did not complete within the timeout period ($TIMEOUT seconds)"
fi

# Shut down the integration service
echo "Shutting down integration service..."
kill -9 $INTEGRATION_PID 2>/dev/null || true

# Kill any remaining Python processes
echo "Cleaning up any remaining processes..."
killall -9 python 2>/dev/null || true

echo "===== Crawl Process Finished ====="
if [ "$completed" = true ]; then
    if [ "$SINGLE_URL" = true ] && [ -n "$URL" ]; then
        echo "URL $URL was crawled successfully!"
    else
        echo "Domain $DOMAIN was crawled successfully!"
    fi
    exit 0
else
    echo "Crawl process did not complete within the timeout period"
    exit 1
fi