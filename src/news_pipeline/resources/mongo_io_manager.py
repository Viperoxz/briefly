from contextlib import contextmanager

from dagster import IOManager, OutputContext, InputContext
from pymongo import MongoClient
import pandas as pd


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

    def _get_collection(self, context):
        db_name = self._config["database"]
        collection_name = context.asset_key.path[-1]
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

        return db[collection_name]

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        collection = self._get_collection(context)
        asset_name = context.asset_key.path[-1]
        records = obj.to_dict(orient="records")

        try:
            if asset_name == "articles":
                client = MongoClient(self._config["uri"])
                db = client[self._config["database"]]
                for record in records:
                    collection.update_one(
                        {"url": record["url"]},  # match by article url link
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
        collection = self._get_collection(context)
        try:
            docs = list(collection.find())
            if docs and "_id" in docs[0]:
                for doc in docs:
                    doc.pop("_id", None)
            return pd.DataFrame(docs)
        except Exception as e:
            raise RuntimeError(f"Failed to load data from MongoDB: {e}")
