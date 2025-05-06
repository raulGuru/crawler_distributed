import logging
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from urllib.parse import urlparse


class MongoDBClient:
    """
    Wrapper for MongoDB client with connection pooling and error handling
    """

    def __init__(self, uri='mongodb://localhost:27017/crawler', connect_timeout=5000, max_retries=3):
        self.uri = uri
        self.connect_timeout = connect_timeout
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None
        self.db = None
        self._connect()

    def _connect(self):
        """Establish connection to MongoDB server"""
        try:
            # Parse database name from URI
            parsed_uri = urlparse(self.uri)
            db_name = parsed_uri.path.strip('/')
            if not db_name:
                db_name = 'crawler'  # Default database name

            # Connect to MongoDB
            self.client = pymongo.MongoClient(
                self.uri,
                connectTimeoutMS=self.connect_timeout,
                serverSelectionTimeoutMS=self.connect_timeout
            )

            # Test connection
            self.client.admin.command('ping')

            # Get database
            self.db = self.client[db_name]

            self.logger.info(f"Connected to MongoDB: {self.uri}")

        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            self.client = None
            self.db = None
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
            self.client = None
            self.db = None
            raise

    def _ensure_connection(self):
        """Ensure connection is established, reconnect if needed"""
        if self.client is None or self.db is None:
            self.logger.warning("MongoDB connection not established, reconnecting...")
            self._connect()

    def _with_retry(self, operation, *args, **kwargs):
        """Execute operation with retry logic"""
        retries = 0
        last_error = None

        while retries <= self.max_retries:
            try:
                self._ensure_connection()
                return operation(*args, **kwargs)
            except (ConnectionFailure, OperationFailure) as e:
                last_error = e
                retries += 1
                if retries <= self.max_retries:
                    self.logger.warning(f"MongoDB operation failed, retrying ({retries}/{self.max_retries}): {str(e)}")
                    # Reset connection for next attempt
                    self.client = None
                    self.db = None
                else:
                    self.logger.error(f"MongoDB operation failed after {retries} retries: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error during MongoDB operation: {str(e)}")
                last_error = e
                break

        # If we get here, all retries failed
        raise last_error if last_error else Exception("Unknown error during MongoDB operation")

    def get_collection(self, collection_name):
        """Get a collection by name"""
        self._ensure_connection()
        return self.db[collection_name]

    def find_one(self, collection_name, query, projection=None):
        """Find a single document in the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            return collection.find_one(query, projection)

        return self._with_retry(operation)

    def find(self, collection_name, query, projection=None, sort=None, limit=0, skip=0):
        """Find documents in the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            cursor = collection.find(query, projection)

            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)

            return list(cursor)

        return self._with_retry(operation)

    def insert_one(self, collection_name, document):
        """Insert a single document into the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.insert_one(document)
            return result.inserted_id

        return self._with_retry(operation)

    def insert_many(self, collection_name, documents):
        """Insert multiple documents into the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.insert_many(documents)
            return result.inserted_ids

        return self._with_retry(operation)

    def update_one(self, collection_name, query, update, upsert=False):
        """Update a single document in the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.update_one(query, update, upsert=upsert)
            return {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': result.upserted_id
            }

        return self._with_retry(operation)

    def update_many(self, collection_name, query, update, upsert=False):
        """Update multiple documents in the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.update_many(query, update, upsert=upsert)
            return {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'upserted_id': result.upserted_id
            }

        return self._with_retry(operation)

    def delete_one(self, collection_name, query):
        """Delete a single document from the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.delete_one(query)
            return result.deleted_count

        return self._with_retry(operation)

    def delete_many(self, collection_name, query):
        """Delete multiple documents from the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            result = collection.delete_many(query)
            return result.deleted_count

        return self._with_retry(operation)

    def count_documents(self, collection_name, query):
        """Count documents in the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            return collection.count_documents(query)

        return self._with_retry(operation)

    def create_index(self, collection_name, keys, **kwargs):
        """Create an index on the collection"""
        def operation():
            collection = self.get_collection(collection_name)
            return collection.create_index(keys, **kwargs)

        return self._with_retry(operation)

    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            try:
                self.client.close()
                self.logger.info("Closed MongoDB connection")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB connection: {str(e)}")
            finally:
                self.client = None
                self.db = None