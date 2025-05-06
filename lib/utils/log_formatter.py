import os
import logging
import traceback


def format_log_message(message, **identifiers):
    """Format a log message with identifiers for easier filtering

    Args:
        message (str): The log message
        **identifiers: Key-value pairs to include in the log message

    Returns:
        str: Formatted log message

    Examples:
        >>> format_log_message("Job completed", job_id="123", crawl_id="abc")
        "[job_id=123] [crawl_id=abc] Job completed"
    """
    # Sort identifiers for consistent ordering
    sorted_ids = sorted(identifiers.items())

    # Format each identifier as [key=value]
    id_strings = [f"[{key}={value}]" for key, value in sorted_ids]

    # Join identifiers with spaces and add to the message
    prefix = " ".join(id_strings)

    if prefix:
        return f"{prefix} {message}"
    else:
        return message


def get_job_specific_logger(base_logger, job_id=None, crawl_id=None, log_dir=None, **other_ids):
    """Create a logger specific to a job with appropriate handlers

    Args:
        base_logger (logging.Logger): Base logger to inherit from
        job_id (str, optional): Job ID
        crawl_id (str, optional): Crawl ID
        log_dir (str, optional): Directory for job-specific log files
        **other_ids: Other identifiers to include

    Returns:
        logging.Logger: Job-specific logger
    """
    # Create a child logger
    logger_name = f"{base_logger.name}"
    if job_id:
        logger_name += f".job_{job_id}"
    if crawl_id:
        logger_name += f".crawl_{crawl_id}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(base_logger.level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # If we have a log directory and identifiers, create a job-specific log file
    if log_dir and (job_id or crawl_id):
        # Create a meaningful filename
        filename_parts = []
        if job_id:
            filename_parts.append(f"job_{job_id}")
        if crawl_id:
            filename_parts.append(f"crawl_{crawl_id}")

        log_filename = "_".join(filename_parts) + ".log"
        log_path = os.path.join(log_dir, log_filename)

        # Ensure directory exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # Create file handler
        file_handler = logging.FileHandler(log_path)

        # Create a formatter that includes all identifiers
        all_ids = {}
        if job_id:
            all_ids['job_id'] = job_id
        if crawl_id:
            all_ids['crawl_id'] = crawl_id
        all_ids.update(other_ids)

        id_parts = " ".join([f"[{k}={v}]" for k, v in sorted(all_ids.items())])
        formatter = logging.Formatter(f'%(asctime)s - %(name)s - {id_parts} - %(levelname)s - %(message)s')

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Create a filter function to add context to all log messages
    class ContextFilter(logging.Filter):
        def filter(self, record):
            # Add attributes to the record
            if job_id:
                record.job_id = job_id
            if crawl_id:
                record.crawl_id = crawl_id
            for key, value in other_ids.items():
                setattr(record, key, value)
            return True

    # Add the filter to the logger
    logger.addFilter(ContextFilter())

    return logger


def log_exception(logger, exception, message=None, **identifiers):
    """Log an exception with traceback and context information

    Args:
        logger (logging.Logger): Logger to use
        exception (Exception): The exception to log
        message (str, optional): Additional message
        **identifiers: Additional context identifiers
    """
    # Format the message with identifiers
    if message:
        formatted_message = format_log_message(message, **identifiers)
    else:
        formatted_message = format_log_message(f"Exception: {str(exception)}", **identifiers)

    # Get the traceback as a string
    tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
    tb_text = "".join(tb_lines)

    # Log the exception with the traceback
    logger.error(f"{formatted_message}\n{tb_text}")


def setup_job_logger(name, job_id=None, crawl_id=None, log_dir=None, level=logging.INFO):
    """Set up a logger for a specific job

    Args:
        name (str): Logger name
        job_id (str, optional): Job ID
        crawl_id (str, optional): Crawl ID
        log_dir (str, optional): Directory for log files
        level (int, optional): Logging level

    Returns:
        logging.Logger: Configured logger
    """
    # Create base logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create identifiers for formatter
    ids = {}
    if job_id:
        ids['job_id'] = job_id
    if crawl_id:
        ids['crawl_id'] = crawl_id

    id_parts = " ".join([f"[{k}={v}]" for k, v in sorted(ids.items())])

    # Create formatters
    if id_parts:
        format_string = f'%(asctime)s - %(name)s - {id_parts} - %(levelname)s - %(message)s'
    else:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    formatter = logging.Formatter(format_string)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create main file handler
    if log_dir:
        # Ensure directory exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # Create a meaningful filename
        filename_parts = [name.lower().replace('.', '_')]
        if job_id:
            filename_parts.append(f"job_{job_id}")
        if crawl_id:
            filename_parts.append(f"crawl_{crawl_id}")

        log_filename = "_".join(filename_parts) + ".log"
        log_path = os.path.join(log_dir, log_filename)

        # Create file handler
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger