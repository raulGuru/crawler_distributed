import json
import logging
from datetime import datetime


class JobSerializer:
    """
    Handles serialization and deserialization of job data for queue operations
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.serialization_format = 'json'
        self.version = '1.0'

    def serialize_job(self, job_data):
        """
        Serialize job data to string format for queue storage

        Args:
            job_data (dict): Job data to serialize

        Returns:
            str: Serialized job data
        """
        if not isinstance(job_data, dict):
            raise ValueError("Job data must be a dictionary")

        # Add metadata
        job_data.update({
            '_meta': {
                'serializer_version': self.version,
                'created_at': datetime.utcnow().isoformat(),
                'format': self.serialization_format
            }
        })

        # Validate required fields based on job type
        self._validate_job_data(job_data)

        try:
            serialized_data = json.dumps(job_data)
            return serialized_data
        except Exception as e:
            self.logger.error(f"Failed to serialize job data: {str(e)}")
            raise

    def deserialize_job(self, serialized_job):
        """
        Deserialize job data from string format

        Args:
            serialized_job (str): Serialized job data

        Returns:
            dict: Deserialized job data
        """
        try:
            job_data = json.loads(serialized_job)

            # Validate job data structure
            if not isinstance(job_data, dict):
                raise ValueError("Deserialized job data is not a dictionary")

            # Check format version
            meta = job_data.get('_meta', {})
            version = meta.get('serializer_version')
            if version and version != self.version:
                self.logger.warning(f"Job serializer version mismatch: {version} vs {self.version}")

            return job_data

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to deserialize job data: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error deserializing job: {str(e)}")
            raise

    def _validate_job_data(self, job_data):
        """
        Validate job data based on job type

        Args:
            job_data (dict): Job data to validate

        Raises:
            ValueError: If job data is invalid
        """
        job_type = job_data.get('job_type')

        if not job_type:
            raise ValueError("Job data missing 'job_type' field")

        # Don't validate fields for crawl_id-only submissions
        # These are meant to be looked up in the database
        if job_data.get('crawl_id') and len(job_data) <= 3:  # crawl_id, job_type, and _meta
            return

        # Validate based on job type
        if job_type == 'crawl':
            # Crawl job requires: domain or url, max_pages, single_url, use_sitemap
            required_fields = []

            # Either domain or URL is required
            if not (job_data.get('domain') or job_data.get('url')):
                required_fields.append('domain or url')

            # Check other required fields
            for field in ['max_pages', 'single_url', 'use_sitemap']:
                if field not in job_data:
                    required_fields.append(field)

            if required_fields:
                raise ValueError(f"Crawl job missing required fields: {', '.join(required_fields)}")

        elif job_type == 'parse':
            # Parse job requires: url, html_file_path
            required_fields = []

            for field in ['url', 'html_file_path']:
                if field not in job_data:
                    required_fields.append(field)

            if required_fields:
                raise ValueError(f"Parse job missing required fields: {', '.join(required_fields)}")

        else:
            self.logger.warning(f"Unknown job type: {job_type}, skipping validation")