from contextlib import contextmanager
from dagster import IOManager, OutputContext, InputContext, io_manager
from pymongo import MongoClient
import pandas as pd
import os

@contextmanager
def connect_mongo(config):
    """Establish a connection to MongoDB and yield the client."""
    client = MongoClient(config["uri"])
    try:
        yield client
    finally:
        client.close()

class MongoDBIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.client = MongoClient(self._config["uri"])
        self.db = self.client[self._config["database"]]

    def _get_collection(self, context, collection_name: str = None):
        # Use provided collection_name if specified, otherwise derive from context
        resolved_collection_name = (
            collection_name
            if collection_name
            else "Articles" if context.asset_key.path[-1] in ["articles", "synced_articles", "summarized_articles"] else context.asset_key.path[-1]
        )

        if resolved_collection_name not in self.db.list_collection_names():
            self.db.create_collection(resolved_collection_name)
            context.log.info(f"Created new collection: {resolved_collection_name}")
            collection = self.db[resolved_collection_name]
            if resolved_collection_name == "Articles":
                collection.create_index("link", unique=True)
            else:
                collection.create_index("name", unique=True)
        return self.db[resolved_collection_name]

    def handle_output(self, context: OutputContext, obj):
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            context.log.info("No data to store in MongoDB")
            return

        if not isinstance(obj, pd.DataFrame):
            raise ValueError(f"Expected pandas DataFrame, got {type(obj)}")

        collection = self._get_collection(context)
        asset_name = context.asset_key.path[-1]
        records = obj.to_dict(orient="records")

        try:
            if asset_name in ["articles", "synced_articles", "summarized_articles"]:
                for record in records:
                    if "link" not in record:
                        context.log.error(f"Record missing 'link' field: {record}")
                        continue
                    # Kiểm tra summary cho summarized_articles
                    if asset_name == "summarized_articles" and ("summary" not in record or not record["summary"]):
                        context.log.error(f"Record missing or empty 'summary' field: {record}")
                        continue
                    collection.update_one(
                        {"link": record["link"]},
                        {"$set": record},  # Lưu toàn bộ record
                        upsert=True
                    )
            elif asset_name in ["sources", "topics"]:
                for record in records:
                    if "name" not in record:
                        context.log.error(f"Record missing 'name' field: {record}")
                        continue
                    collection.update_one(
                        {"name": record["name"]},
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
            query = {}
            if context.has_partition_key:
                query["link"] = context.partition_key
            docs = list(collection.find(query))
            context.log.info(f"Loaded {len(docs)} documents for asset {context.asset_key.path[-1]}")
            if docs and "_id" in docs[0]:
                for doc in docs:
                    doc.pop("_id", None)
            df = pd.DataFrame(docs)
            context.log.info(f"Returning DataFrame with {len(df)} rows")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load data from MongoDB: {e}")

    @property
    def client(self):
        """Expose MongoClient for use in other resources."""
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

@io_manager
def mongo_io_manager():
    return MongoDBIOManager(
        config={
            "uri": os.getenv("MONGO_URI"),
            "database": os.getenv("MONGO_DB")
        }
    )