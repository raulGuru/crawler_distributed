#!/bin/bash

# Run the integration service for the distributed crawler
# This script starts the integration service which manages all worker processes

# Get the project root directory
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Set environment variables
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
export LOG_DIR="$PROJECT_ROOT/data/logs"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Default configuration
QUEUE_HOST="localhost"
QUEUE_PORT=11300
DB_URI="mongodb://localhost:27017/crawler"
HEALTH_CHECK_INTERVAL=60

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --queue-host)
      QUEUE_HOST="$2"
      shift 2
      ;;
    --queue-port)
      QUEUE_PORT="$2"
      shift 2
      ;;
    --db-uri)
      DB_URI="$2"
      shift 2
      ;;
    --health-check-interval)
      HEALTH_CHECK_INTERVAL="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --queue-host HOST            Beanstalkd host (default: localhost)"
      echo "  --queue-port PORT            Beanstalkd port (default: 11300)"
      echo "  --db-uri URI                 MongoDB URI (default: mongodb://localhost:27017/crawler)"
      echo "  --health-check-interval SEC  Health check interval in seconds (default: 60)"
      echo "  --help                       Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Log startup
echo "Starting integration service at $(date)"
echo "Project root: $PROJECT_ROOT"
echo "Queue host: $QUEUE_HOST"
echo "Queue port: $QUEUE_PORT"
echo "DB URI: $DB_URI"
echo "Health check interval: $HEALTH_CHECK_INTERVAL seconds"

# Command to run
CMD="python $PROJECT_ROOT/workers/integration_service.py --queue-host $QUEUE_HOST --queue-port $QUEUE_PORT --db-uri $DB_URI --health-check-interval $HEALTH_CHECK_INTERVAL"

echo "Command: $CMD"
echo "------------------------------------------------------------------------------"

# Run the integration service
$CMD

# Get exit code
EXIT_CODE=$?

# Log exit
echo "------------------------------------------------------------------------------"
echo "Integration service exited with code $EXIT_CODE at $(date)"

exit $EXIT_CODE