# check_mongo.py

from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

# load vars from .env
load_dotenv()

# read config with sensible defaults
HOST         = os.getenv("MONGO_HOST", "localhost")
PORT         = int(os.getenv("MONGO_PORT", 27017))
DB_NAME      = os.getenv("MONGO_DB", "crawler")
USER         = os.getenv("MONGO_USER") or None
PASSWORD     = os.getenv("MONGO_PASSWORD") or None
AUTH_SOURCE  = os.getenv("MONGO_AUTH_SOURCE", "admin")

# build client args
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

try:
    client = MongoClient(**client_args)
    # verify connection & auth
    client.admin.command("ping")
    print("✅ Connected successfully to MongoDB")
except (ConnectionFailure, ConfigurationError, OperationFailure) as e:
    print(f"❌ MongoDB connection/authentication failed:\n{e}")
    exit(1)

# use the target DB
db = client[DB_NAME]

print("Collections:", db.list_collection_names())

jobs = list(db["crawl_jobs"].find())
print(f"Crawl jobs: {len(jobs)}")
if jobs:
    print("Most recent job:", jobs[-1])
else:
    print("No documents found in 'crawl_jobs'.")
