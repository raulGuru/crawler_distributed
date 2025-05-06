import os
import logging
import json
from datetime import datetime
from functools import wraps
import traceback
import sys

from config.base_settings import LOG_DIR


class LoggingUtils:
    """
    Provides utilities for structured logging across the distributed crawler system
    """

    @staticmethod
    def setup_logger(name, log_file=None, level=logging.INFO, console=True):
        """
        Set up a logger with file and optional console handlers

        Args:
            name (str): Logger name
            log_file (str, optional): Log file path. If None, uses name.log
            level (int, optional): Logging level
            console (bool, optional): Whether to add console handler

        Returns:
            logging.Logger: Configured logger
        """
        # Create log directory if it doesn't exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR, exist_ok=True)

        # Set up logger
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Remove any existing handlers
        while logger.handlers:
            logger.handlers.pop()

        # Determine log file if not provided
        if log_file is None:
            log_file = os.path.join(LOG_DIR, f"{name}.log")

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Create console handler if requested
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    @staticmethod
    def log_exception(logger, e, message=None):
        """
        Log an exception with traceback

        Args:
            logger (logging.Logger): Logger to use
            e (Exception): Exception to log
            message (str, optional): Additional message
        """
        exc_info = sys.exc_info()
        tb_lines = traceback.format_exception(*exc_info)
        tb_str = ''.join(tb_lines)

        if message:
            logger.error(f"{message}: {str(e)}\n{tb_str}")
        else:
            logger.error(f"Exception: {str(e)}\n{tb_str}")

    @staticmethod
    def log_context(func=None, logger=None, level=logging.INFO):
        """
        Decorator to log function entry and exit with parameters and results

        Args:
            func (callable, optional): Function to decorate
            logger (logging.Logger, optional): Logger to use, if None uses function module logger
            level (int, optional): Logging level

        Returns:
            callable: Decorated function
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Get logger if not provided
                nonlocal logger
                if logger is None:
                    logger = logging.getLogger(func.__module__)

                # Format arguments, but avoid excessive logging
                args_str = str(args) if len(str(args)) < 200 else f"{str(args)[:200]}..."
                kwargs_str = str(kwargs) if len(str(kwargs)) < 200 else f"{str(kwargs)[:200]}..."

                # Log function entry
                logger.log(level, f"ENTER {func.__name__} - args: {args_str}, kwargs: {kwargs_str}")

                try:
                    # Call the function
                    result = func(*args, **kwargs)

                    # Log function exit
                    result_str = str(result) if len(str(result)) < 200 else f"{str(result)[:200]}..."
                    logger.log(level, f"EXIT {func.__name__} - result: {result_str}")

                    return result
                except Exception as e:
                    # Log the exception
                    LoggingUtils.log_exception(logger, e, f"ERROR in {func.__name__}")
                    raise

            return wrapper

        if func is None:
            # Called with parameters: @log_context(logger=my_logger)
            return decorator
        else:
            # Called without parameters: @log_context
            return decorator(func)

    @staticmethod
    def format_json(obj):
        """
        Format an object as pretty JSON with handling for non-serializable types

        Args:
            obj (object): Object to format

        Returns:
            str: Formatted JSON string
        """
        class CustomEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)

        return json.dumps(obj, indent=2, cls=CustomEncoder)

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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
            formatted_message = LoggingUtils.format_log_message(message, **identifiers)
        else:
            formatted_message = LoggingUtils.format_log_message(f"Exception: {str(exception)}", **identifiers)

        # Get the traceback as a string
        tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
        tb_text = "".join(tb_lines)

        # Log the exception with the traceback
        logger.error(f"{formatted_message}\n{tb_text}")