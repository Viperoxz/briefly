from dagster import resource
from pymongo import MongoClient
import os

@resource(config_schema={"uri": str, "db": str})
def mongo_db_resource(init_context):
    mongo_uri = init_context.resource_config.get("uri") or os.getenv("MONGO_URI")
    mongo_db = init_context.resource_config.get("db") or os.getenv("MONGO_DB")
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    
    try:
        yield db
    finally:
        client.close()