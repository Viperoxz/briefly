from contextlib import contextmanager
from dagster import IOManager, OutputContext, InputContext
from pymongo import MongoClient
import pandas as pd
import json
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


@contextmanager
def connect_mongo(config):
    """Establish a connection to MongoDB and yield the client."""
    client = MongoClient(config["uri"])
    try:
        yield client
    except Exception as e:
        raise e


class MongoDBIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_collection(self, context, collection_name=None):
        if not collection_name:
            collection_name = context.asset_key.path[-1]

        allowed_collections = ["articles", "topics", "sources", "rss_feed_list"]
        if collection_name not in allowed_collections:
            print(f"Warning: Attempting to access restricted collection: {collection_name}")
            collection_name = "articles" 
        
        db_name = self._config["database"]
        client = MongoClient(self._config["uri"])
        db = client[db_name]

        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"Created new collection: {collection_name}")
            collection = db[collection_name]

            # Create appropriate indexes based on collection type
            if collection_name == "articles":
                collection.create_index("url", unique=True)
            elif collection_name == "topics":
                collection.create_index("name", unique=True)
            elif collection_name == "sources":
                collection.create_index("name", unique=True)
            # elif collection_name == "summarized_articles":
            #     collection.create_index("url", unique=True)

        return db[collection_name]

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        asset_name = context.asset_key.path[-1]
        allowed_collections = ["articles", "topics", "sources", "rss_feed_list"]
        if asset_name not in allowed_collections:
            context.log.info(f"Skipping MongoDB storage for {asset_name} (not in allowed collections)")
            return
        collection = self._get_collection(context, asset_name)
        records = []

        try:
            if asset_name == "rss_feed_list":
                if isinstance(obj, dict):
                    collection.delete_many({})
                    collection.insert_one(obj)
                    context.log.info(f"✅ Stored RSS feed list in MongoDB collection '{asset_name}'")
                else:
                # If dataframe
                    records = obj.to_dict(orient="records")
                    record = records[0] if records else {}
                    collection.delete_many({})
                    collection.insert_one(record)
                    context.log.info(f"✅ Stored RSS feed list in MongoDB collection '{asset_name}'")
            if isinstance(obj, pd.DataFrame):
                records = obj.to_dict(orient="records")
                if asset_name == "articles":
                    client = MongoClient(self._config["uri"])
                    db = client[self._config["database"]]
                    for record in records:
                        collection.update_one(
                            {"url": record["url"]},  # match by article url 
                            {"$set": record},
                            upsert=True
                        )
                elif asset_name == "sources":
                    for record in records:
                        collection.update_one(
                            {"name": record["name"]},  # match by source name
                            {"$set": record},
                            upsert=True
                        )
                elif asset_name == "topics":
                    # For topics, use name as unique identifier
                    for record in records:
                        collection.update_one(
                            {"name": record["name"]},  # match by topic name
                            {"$set": record},
                            upsert=True
                        )
                else:
                    collection.delete_many({})
                    if records:
                        collection.insert_many(records)

            context.log.info(f"✅ Stored {len(records)} records in MongoDB collection '{asset_name}'")
        except Exception as e:
            context.log.error(f"❌ Failed to store in MongoDB: {e}")
            raise RuntimeError(f"Failed to insert data into MongoDB: {e}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        collection_name = context.asset_key.path[-1]
        collection = self._get_collection(context, collection_name)

        try:
            if collection_name == "rss_feed_list":
                doc = collection.find_one()
                if doc:
                    if "_id" in doc:
                        doc.pop("_id", None)
                    return doc
                else:
                    base_dir = Path(__file__).resolve().parent.parent.parent.parent
                    file_path = base_dir / "config" / "rss_feeds.json"
                    with open(file_path, "r", encoding="utf-8") as f:
                        return json.load(f)  
            else:
                docs = list(collection.find())
                if docs and "_id" in docs[0]:
                    for doc in docs:
                        doc.pop("_id", None)
                return pd.DataFrame(docs)
        except Exception as e:
            raise RuntimeError(f"Failed to load data from MongoDB: {e}")
        

def validate_mongo_connection():
    try:
        client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=5000)
        client.server_info()  
        return True
    except Exception:
        return False
        
