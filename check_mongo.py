# check_mongo.py

from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

# 1) load .env
load_dotenv()

# 2) read config
HOST         = os.getenv("MONGO_HOST", "localhost")
PORT         = int(os.getenv("MONGO_PORT", 27017))
DB_NAME      = os.getenv("MONGO_DB", "crawler")
USER         = os.getenv("MONGO_USER") or None
PASSWORD     = os.getenv("MONGO_PASSWORD") or None
AUTH_SOURCE  = os.getenv("MONGO_AUTH_SOURCE", "admin")

# new: allow overriding the collection name
JOB_COLL     = os.getenv("MONGO_CRAWL_JOB_COLLECTION", "crawl_jobs")

# 3) build connection args
client_args = {
    "host": HOST,
    "port": PORT,
    "serverSelectionTimeoutMS": 5000,
}
if USER and PASSWORD:
    client_args.update({
        "username": USER,
        "password": PASSWORD,
        "authSource": AUTH_SOURCE,
        "authMechanism": "SCRAM-SHA-1",
    })

# 4) connect & auth
try:
    client = MongoClient(**client_args)
    client.admin.command("ping")
    print("✅ Connected successfully to MongoDB")
except (ConnectionFailure, ConfigurationError, OperationFailure) as e:
    print(f"❌ MongoDB connection/authentication failed:\n{e}")
    exit(1)

# 5) select the right DB & collection
db = client[DB_NAME]
print("Collections:", db.list_collection_names())

jobs = list(db[JOB_COLL].find())
print(f"Crawl jobs in `{JOB_COLL}`: {len(jobs)}")
if jobs:
    print("Most recent job:", jobs[-1])
else:
    print(f"No documents found in `{JOB_COLL}`.")
