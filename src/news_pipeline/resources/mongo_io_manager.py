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

    def _get_collection(self, context, collection_name=None):
        if not collection_name:
            collection_name = context.asset_key.path[-1]
        
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
            elif collection_name == "summarized_articles":
                collection.create_index("url", unique=True)
            elif collection_name == "audio_summaries":
                collection.create_index("url", unique=True)

        return db[collection_name]

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        asset_name = context.asset_key.path[-1]
        # Skip MongoDB storage for embedded_articles
        if asset_name == "embedded_articles":
            context.log.info("Skipping MongoDB storage for embedded_articles (stored in Qdrant)")
            return
        collection = self._get_collection(context, asset_name)
        records = obj.to_dict(orient="records")

        try:
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
            elif asset_name == "summarized_articles":
                for record in records:
                    collection.update_one(
                        {"url": record["url"]},  # match by article url
                        {"$set": {
                            "url": record["url"],
                            "title": record["title"],
                            "published_date": record["published_date"],
                            "source_id": record["source_id"],
                            "topic_id": record["topic_id"],
                            "summary": record["summary"]
                        }},
                        upsert=True
                    )
            elif asset_name == "audio_summaries":
                for record in records:
                    collection.update_one(
                        {"url": record["url"]},  # Match by article url
                        {"$set": {
                            "url": record["url"],
                            "title": record["title"],
                            "published_date": record["published_date"],
                            "source_id": record["source_id"],
                            "topic_id": record["topic_id"],
                            "summary_for_audio": record["summary_for_audio"],
                            "audio_url": record["audio_url"]
                        }},
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
        
