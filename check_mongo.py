from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/crawler')
db = client.get_database()
print('Collections:', db.list_collection_names())
crawl_jobs = list(db['crawl_jobs'].find())
print(f'Crawl jobs: {len(crawl_jobs)}')
for job in crawl_jobs[-1:]:
    print(job)