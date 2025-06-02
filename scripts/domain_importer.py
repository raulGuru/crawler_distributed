#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Domain CSV to MongoDB Importer

This script reads domain names from a CSV file and inserts them into
the domains_crawl MongoDB collection. It follows the existing codebase
patterns for error handling, logging, and database operations.
"""

import os
import sys
import csv
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lib.storage.mongodb_client import MongoDBClient
from lib.utils.logging_utils import LoggingUtils


class DomainImporter:
    """
    Handles importing domain names from CSV files into MongoDB.

    This class follows the Single Responsibility Principle by focusing
    solely on the domain import functionality.
    """

    def __init__(self, csv_file_path: str, collection_name: str = 'domains_crawl'):
        """
        Initialize the domain importer.

        Args:
            csv_file_path (str): Path to the CSV or text file containing domain names
            collection_name (str): MongoDB collection name (default: 'domains_crawl')
        """
        self.csv_file_path = Path(csv_file_path)
        self.collection_name = collection_name

        # Ensure logs directory exists
        os.makedirs('data/logs', exist_ok=True)

        self.logger = self._setup_logging()
        self.mongodb_client = None

        # Validate file exists
        if not self.csv_file_path.exists():
            raise FileNotFoundError(f"Input file not found: {csv_file_path}")

    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging using the existing logging utility.

        Returns:
            logging.Logger: Configured logger instance
        """
        # Create log file path following the existing pattern
        log_file = os.path.join('data', 'logs', 'domain_importer.log')

        logger = LoggingUtils.setup_logger(
            name="domain_importer",
            log_file=log_file,
            level=None,
            console=True,
            json_format=False,
        )
        logger.propagate = False
        return logger

    def _read_domains_from_csv(self, domain_column: str = 'domain') -> List[str]:
        """
        Read domain names from CSV file or simple text file.

        Args:
            domain_column (str): Name of the column containing domain names

        Returns:
            List[str]: List of domain names

        Raises:
            ValueError: If domain column not found or file is invalid
        """
        domains = []

        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                # Read a sample to determine file format
                sample = file.read(1024)
                file.seek(0)

                # Check if this looks like a simple text file (one domain per line)
                if '\n' in sample and (',' not in sample and ';' not in sample and '\t' not in sample):
                    self.logger.info("Detected simple text file format (one domain per line)")
                    for line_num, line in enumerate(file, start=1):
                        domain = line.strip()
                        if domain and not domain.startswith('#'):  # Skip comments
                            domains.append(domain)
                        elif not domain:
                            self.logger.debug(f"Empty line found at line {line_num}")

                    self.logger.info(f"Read {len(domains)} domains from text file")

                else:
                    # Try to process as CSV
                    file.seek(0)
                    try:
                        # Try to detect delimiter and headers
                        sniffer = csv.Sniffer()
                        delimiter = sniffer.sniff(sample).delimiter
                        has_header = sniffer.has_header(sample)

                        self.logger.info(f"Detected CSV format with delimiter '{delimiter}', headers: {has_header}")

                    except csv.Error:
                        # If sniffer fails, assume comma-separated or single column
                        delimiter = ',' if ',' in sample else None
                        has_header = False

                        # Check if first line looks like a header
                        first_line = file.readline().strip()
                        file.seek(0)

                        if (not delimiter and
                            first_line and
                            not self._looks_like_domain(first_line)):
                            has_header = True
                            self.logger.info("Detected single column with header")
                        else:
                            self.logger.info("Detected single column without header")

                    # Read the CSV data
                    file.seek(0)

                    if delimiter:
                        # Multi-column CSV
                        reader = csv.DictReader(file, delimiter=delimiter) if has_header else csv.reader(file, delimiter=delimiter)

                        if has_header:
                            # Use column name to extract domains
                            if domain_column not in reader.fieldnames:
                                available_columns = ', '.join(reader.fieldnames)
                                raise ValueError(
                                    f"Column '{domain_column}' not found. "
                                    f"Available columns: {available_columns}"
                                )

                            for row_num, row in enumerate(reader, start=2):  # Start at 2 due to header
                                domain = row.get(domain_column, '').strip()
                                if domain:
                                    domains.append(domain)
                                elif domain == '':
                                    self.logger.warning(f"Empty domain found in row {row_num}")
                        else:
                            # Assume first column contains domains
                            for row_num, row in enumerate(reader, start=1):
                                if row and len(row) > 0:
                                    domain = row[0].strip()
                                    if domain:
                                        domains.append(domain)
                                    else:
                                        self.logger.warning(f"Empty domain found in row {row_num}")
                    else:
                        # Single column without delimiter
                        if has_header:
                            # Skip first line (header)
                            file.readline()

                        for line_num, line in enumerate(file, start=2 if has_header else 1):
                            domain = line.strip()
                            if domain and not domain.startswith('#'):  # Skip comments
                                domains.append(domain)
                            elif not domain:
                                self.logger.debug(f"Empty line found at line {line_num}")

        except Exception as e:
            self.logger.error(f"Error reading file: {str(e)}")
            LoggingUtils.log_exception(self.logger, e, "File reading failed")
            raise

        # Remove duplicates while preserving order
        unique_domains = list(dict.fromkeys(domains))

        if len(domains) != len(unique_domains):
            duplicates_removed = len(domains) - len(unique_domains)
            self.logger.info(f"Removed {duplicates_removed} duplicate domain(s)")

        self.logger.info(f"Successfully read {len(unique_domains)} unique domains from file")
        return unique_domains

    def _looks_like_domain(self, text: str) -> bool:
        """
        Check if a text string looks like a domain name.

        Args:
            text (str): Text to check

        Returns:
            bool: True if text looks like a domain
        """
        text = text.strip().lower()

        # Basic domain pattern check
        if '.' not in text:
            return False

        # Remove common prefixes
        if text.startswith(('http://', 'https://')):
            text = text.split('//', 1)[1]

        # Check for basic domain structure
        parts = text.split('.')
        if len(parts) < 2:
            return False

        # Very basic validation - just check it's not obviously a header
        return not any(word in text for word in ['domain', 'website', 'url', 'site', 'name'])

    def _prepare_domain_documents(self, domains: List[str], url_crawl: bool = False) -> List[Dict[str, Any]]:
        """
        Prepare domain documents for MongoDB insertion.

        Args:
            domains (List[str]): List of domain names

        Returns:
            List[Dict[str, Any]]: List of domain documents ready for insertion
        """
        current_time = datetime.utcnow()
        documents = []

        for domain in domains:
            # Normalize domain (remove www prefix, convert to lowercase)
            normalized_domain = self._normalize_domain(domain)
            # Default values for crawl parameters
            max_pages = 25
            use_sitemap = True
            single_url = False
            url = None
            if url_crawl:
                max_pages = 1
                use_sitemap = False
                single_url = True
                url = f'https://{normalized_domain}'

            document = {
                'domain': normalized_domain,
                'original_domain': domain,  # Keep original for reference
                'status': 'new',
                'max_pages': max_pages,
                'single_url': single_url,
                'use_sitemap': use_sitemap,
                'url': url,
                'cycle_id': 1,
                'metadata': {
                    'source': 'csv_import',
                    'import_batch': current_time.strftime('%Y%m%d_%H%M%S')
                }
            }
            documents.append(document)

        return documents

    def _normalize_domain(self, domain: str) -> str:
        """
        Normalize domain name for consistent storage.

        Args:
            domain (str): Raw domain name

        Returns:
            str: Normalized domain name
        """
        # Remove protocol if present
        domain = domain.replace('http://', '').replace('https://', '')

        # Remove trailing slash
        domain = domain.rstrip('/')

        # Convert to lowercase
        domain = domain.lower().strip()

        # Remove www prefix for consistency
        if domain.startswith('www.'):
            domain = domain[4:]

        return domain

    def _insert_domains(self, documents: List[Dict[str, Any]],
                       batch_size: int = 100) -> Dict[str, int]:
        """
        Insert domain documents into MongoDB with batch processing.

        Args:
            documents (List[Dict[str, Any]]): Domain documents to insert
            batch_size (int): Number of documents to insert per batch

        Returns:
            Dict[str, int]: Statistics about the insertion process
        """
        stats = {
            'total_processed': 0,
            'successfully_inserted': 0,
            'duplicates_skipped': 0,
            'errors': 0
        }

        # Process documents in batches to avoid memory issues
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            stats['total_processed'] += len(batch)

            # Process each document in the batch
            for doc in batch:
                try:
                    # Check if domain already exists
                    existing = self.mongodb_client.find_one(
                        self.collection_name,
                        {'domain': doc['domain']}
                    )

                    if existing:
                        stats['duplicates_skipped'] += 1
                        self.logger.debug(f"Domain already exists, skipping: {doc['domain']}")
                    else:
                        # Insert new domain
                        self.mongodb_client.insert_one(self.collection_name, doc)
                        stats['successfully_inserted'] += 1
                        self.logger.debug(f"Inserted domain: {doc['domain']}")

                except Exception as doc_error:
                    stats['errors'] += 1
                    self.logger.error(
                        f"Error inserting domain {doc['domain']}: {str(doc_error)}"
                    )

            # Log progress for large batches
            if len(documents) > batch_size:
                progress = min(i + batch_size, len(documents))
                self.logger.info(f"Processed {progress}/{len(documents)} domains")

        return stats

    def import_domains(self, domain_column: str = 'domain',
                      batch_size: int = 100,
                      url_crawl: bool = False) -> Dict[str, int]:
        """
        Main method to import domains from CSV to MongoDB.

        Args:
            domain_column (str): Name of the CSV column containing domains
            batch_size (int): Number of documents to process per batch

        Returns:
            Dict[str, int]: Import statistics
        """
        self.logger.info(f"Starting domain import from {self.csv_file_path}")

        try:
            # Initialize MongoDB connection
            self.mongodb_client = MongoDBClient(logger=self.logger)

            # Read domains from file (CSV or text)
            domains = self._read_domains_from_csv(domain_column)

            if not domains:
                self.logger.warning("No domains found in input file")
                return {'total_processed': 0, 'successfully_inserted': 0,
                       'duplicates_skipped': 0, 'errors': 0}

            # Prepare documents for insertion
            documents = self._prepare_domain_documents(domains, url_crawl)

            # Insert documents into MongoDB
            try:
                stats = self._insert_domains(documents, batch_size)
            except Exception as insert_error:
                self.logger.error(f"Error during domain insertion: {str(insert_error)}")
                LoggingUtils.log_exception(self.logger, insert_error, "Domain insertion failed")
                raise

            # Log final statistics
            self.logger.info(f"Import completed. Statistics: {stats}")

            return stats

        except Exception as e:
            self.logger.error(f"Import failed: {str(e)}")
            LoggingUtils.log_exception(self.logger, e, "Domain import failed")
            raise

        finally:
            # Clean up MongoDB connection
            if self.mongodb_client:
                try:
                    self.mongodb_client.close()
                except Exception as e:
                    self.logger.error(f"Error closing MongoDB connection: {str(e)}")


def main():
    """
    Main entry point for the domain importer script.
    """
    parser = argparse.ArgumentParser(
        description='Import domain names from CSV or text file to MongoDB'
    )
    parser.add_argument(
        'csv_file',
        help='Path to the CSV or text file containing domain names'
    )
    parser.add_argument(
        '--column',
        default='domain',
        help='Name of the CSV column containing domain names (default: domain)'
    )
    parser.add_argument(
        '--collection',
        default='domains_crawl',
        help='MongoDB collection name (default: domains_crawl)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of documents to process per batch (default: 100)'
    )
    parser.add_argument(
        '--url-crawl',
        default=False,
        help='Crawl the URL for the domain (default: False)'
    )

    args = parser.parse_args()

    try:
        # Create importer instance
        importer = DomainImporter(
            csv_file_path=args.csv_file,
            collection_name=args.collection
        )

        # Import domains
        stats = importer.import_domains(
            domain_column=args.column,
            batch_size=args.batch_size,
            url_crawl=args.url_crawl
        )

        # Print results
        print(f"\n=== Import Results ===")
        print(f"Total processed: {stats['total_processed']}")
        print(f"Successfully inserted: {stats['successfully_inserted']}")
        print(f"Duplicates skipped: {stats['duplicates_skipped']}")
        print(f"Errors: {stats['errors']}")

        if stats['errors'] > 0:
            print(f"\nSome errors occurred during import. Check logs for details.")
            sys.exit(1)
        else:
            print(f"\nImport completed successfully!")
            sys.exit(0)

    except FileNotFoundError as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()