import os
import logging
import hashlib
import shutil
import fcntl
import errno
from urllib.parse import urlparse
from datetime import datetime


class FileStorage:
    """
    Manages HTML file storage system
    """

    def __init__(self, base_dir='data/html', create_dirs=True):
        self.base_dir = base_dir
        self.logger = logging.getLogger(self.__class__.__name__)

        if create_dirs:
            self._ensure_base_dir()

    def _ensure_base_dir(self):
        """Ensure base directory exists"""
        if not os.path.exists(self.base_dir):
            try:
                os.makedirs(self.base_dir, exist_ok=True)
                self.logger.info(f"Created base directory: {self.base_dir}")
            except Exception as e:
                self.logger.error(f"Failed to create base directory: {str(e)}")
                raise

    def _get_domain_dir(self, url):
        """Get domain directory for a URL"""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        # Replace characters that might cause issues in file paths
        domain = domain.replace(':', '_')

        domain_dir = os.path.join(self.base_dir, domain)
        return domain_dir

    def _ensure_domain_dir(self, url):
        """Ensure domain directory exists for a URL"""
        domain_dir = self._get_domain_dir(url)

        if not os.path.exists(domain_dir):
            try:
                os.makedirs(domain_dir, exist_ok=True)
                self.logger.debug(f"Created domain directory: {domain_dir}")
            except Exception as e:
                self.logger.error(f"Failed to create domain directory: {str(e)}")
                raise

        return domain_dir

    def generate_file_path(self, url, timestamp=None):
        """
        Generate a file path for storing HTML content

        Args:
            url (str): URL of the page
            timestamp (str/datetime, optional): Timestamp for the file

        Returns:
            str: File path
        """
        # Parse URL
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        path = parsed_url.path.strip('/')
        query = parsed_url.query

        # Base file name on URL path
        if not path:
            path = 'index'
        else:
            # Replace slashes with underscores
            path = path.replace('/', '_')

        # Add hash of query parameters if present
        if query:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            path = f"{path}_q{query_hash}"

        # Add timestamp
        if timestamp is None:
            timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        elif isinstance(timestamp, datetime):
            timestamp = timestamp.strftime('%Y%m%d%H%M%S')
        elif isinstance(timestamp, str) and 'T' in timestamp:
            # Convert ISO format to compact format
            timestamp = timestamp.replace('-', '').replace(':', '').replace('T', '').split('.')[0]

        file_name = f"{path}_{timestamp}.html"

        # Limit file name length
        if len(file_name) > 200:
            file_name_hash = hashlib.md5(file_name.encode()).hexdigest()[:8]
            file_name = f"{file_name[:190]}_{file_name_hash}.html"

        # Ensure domain directory exists
        domain_dir = self._ensure_domain_dir(url)

        # Return full file path
        return os.path.join(domain_dir, file_name)

    def store_html(self, url, content, timestamp=None, metadata=None):
        """
        Store HTML content to file

        Args:
            url (str): URL of the page
            content (bytes/str): HTML content
            timestamp (str/datetime, optional): Timestamp for the file
            metadata (dict, optional): Metadata to store with the file

        Returns:
            str: File path
        """
        file_path = self.generate_file_path(url, timestamp)

        try:
            # Make sure content is bytes
            if isinstance(content, str):
                content = content.encode('utf-8')

            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Write content to file with lock
            with open(file_path, 'wb') as f:
                # Try to acquire an exclusive lock
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError as e:
                    if e.errno == errno.EAGAIN:
                        self.logger.warning(f"File {file_path} is locked, waiting for lock...")
                        fcntl.flock(f, fcntl.LOCK_EX)  # Wait for lock
                    else:
                        raise

                f.write(content)

                # Write metadata if provided
                if metadata:
                    meta_path = f"{file_path}.meta"
                    try:
                        import json
                        with open(meta_path, 'w') as mf:
                            json.dump(metadata, mf)
                    except Exception as e:
                        self.logger.error(f"Failed to write metadata: {str(e)}")

                # Release lock
                fcntl.flock(f, fcntl.LOCK_UN)

            self.logger.debug(f"Stored HTML content to {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to store HTML content: {str(e)}")
            raise

    def read_html(self, file_path):
        """
        Read HTML content from file

        Args:
            file_path (str): Path to HTML file

        Returns:
            bytes: HTML content
        """
        try:
            with open(file_path, 'rb') as f:
                # Try to acquire a shared lock
                try:
                    fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except IOError as e:
                    if e.errno == errno.EAGAIN:
                        self.logger.warning(f"File {file_path} is locked, waiting for lock...")
                        fcntl.flock(f, fcntl.LOCK_SH)  # Wait for lock
                    else:
                        raise

                content = f.read()

                # Release lock
                fcntl.flock(f, fcntl.LOCK_UN)

            return content

        except FileNotFoundError:
            self.logger.error(f"HTML file not found: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to read HTML content: {str(e)}")
            return None

    def delete_html(self, file_path):
        """
        Delete HTML file

        Args:
            file_path (str): Path to HTML file

        Returns:
            bool: True if deleted, False otherwise
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)

                # Delete metadata file if it exists
                meta_path = f"{file_path}.meta"
                if os.path.exists(meta_path):
                    os.remove(meta_path)

                self.logger.debug(f"Deleted HTML file: {file_path}")
                return True
            else:
                self.logger.warning(f"HTML file not found for deletion: {file_path}")
                return False

        except Exception as e:
            self.logger.error(f"Failed to delete HTML file: {str(e)}")
            return False

    def list_files(self, domain=None, max_files=50):
        """
        List HTML files

        Args:
            domain (str, optional): Domain to list files for
            max_files (int, optional): Maximum number of files to return

        Returns:
            list: List of file paths
        """
        files = []

        try:
            if domain:
                # List files for specific domain
                domain_dir = os.path.join(self.base_dir, domain)
                if os.path.exists(domain_dir):
                    for file_name in os.listdir(domain_dir):
                        if file_name.endswith('.html') and not file_name.endswith('.meta'):
                            files.append(os.path.join(domain_dir, file_name))
                            if len(files) >= max_files:
                                break
            else:
                # List all files in all domains
                for domain in os.listdir(self.base_dir):
                    domain_dir = os.path.join(self.base_dir, domain)
                    if os.path.isdir(domain_dir):
                        for file_name in os.listdir(domain_dir):
                            if file_name.endswith('.html') and not file_name.endswith('.meta'):
                                files.append(os.path.join(domain_dir, file_name))
                                if len(files) >= max_files:
                                    break

            return files

        except Exception as e:
            self.logger.error(f"Failed to list HTML files: {str(e)}")
            return []

    def cleanup_old_files(self, days=30, domain=None):
        """
        Delete HTML files older than a certain number of days

        Args:
            days (int, optional): Number of days
            domain (str, optional): Domain to clean up

        Returns:
            int: Number of files deleted
        """
        deleted_count = 0
        now = datetime.utcnow()

        try:
            files = self.list_files(domain=domain, max_files=1000000)

            for file_path in files:
                try:
                    file_stat = os.stat(file_path)
                    file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
                    age_days = (now - file_mtime).days

                    if age_days > days:
                        if self.delete_html(file_path):
                            deleted_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path} during cleanup: {str(e)}")

            self.logger.info(f"Cleaned up {deleted_count} HTML files older than {days} days")
            return deleted_count

        except Exception as e:
            self.logger.error(f"Failed to clean up old HTML files: {str(e)}")
            return 0