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
        self._client = MongoClient(self._config["uri"])
        self.db = self._client[self._config["database"]]
        self._initialize_collections()

    def _initialize_collections(self):
        """Initialize collections and manage indexes."""
        collections = ["articles", "summarized_articles", "synced_articles", "embedded_articles", "sources", "topics", "checked_summaries", "clean_orphaned_embeddings"]
        for coll_name in collections:
            if coll_name not in self.db.list_collection_names():
                self.db.create_collection(coll_name)
            collection = self.db[coll_name]
            if coll_name == "articles":
                # Drop existing name_1 index and create unique index on link
                if "name_1" in collection.index_information():
                    collection.drop_index("name_1")
                collection.create_index("link", unique=True)
            elif coll_name in ["sources", "topics"]:
                collection.create_index("name", unique=True)
            else:
                # Ensure no unintended indexes (e.g., name_1) exist on other collections
                if "name_1" in collection.index_information():
                    collection.drop_index("name_1")

    def _get_collection(self, context, collection_name: str = None):
        if collection_name:
            resolved_collection_name = collection_name
        elif hasattr(context, 'asset_key_for_output'):
            outs = context.op_config.get("outs", {}) if context.op_config else {}
            output_name = list(outs.keys())[0] if outs else "embedded_articles"
            resolved_collection_name = context.asset_key_for_output(output_name).path[-1]
        elif hasattr(context, 'asset_key'):
            resolved_collection_name = context.asset_key.path[-1]
        else:
            resolved_collection_name = "articles"

        if resolved_collection_name not in self.db.list_collection_names():
            self.db.create_collection(resolved_collection_name)
            context.log.info(f"Created new collection: {resolved_collection_name}")
            collection = self.db[resolved_collection_name]
            if resolved_collection_name == "articles":
                collection.create_index("link", unique=True)
            elif resolved_collection_name in ["sources", "topics"]:
                collection.create_index("name", unique=True)
        return self.db[resolved_collection_name]

    def handle_output(self, context: OutputContext, obj):
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            context.log.info("No data to store in MongoDB")
            return

        if not isinstance(obj, pd.DataFrame):
            raise ValueError(f"Expected pandas DataFrame, got {type(obj)}")

        collection = self._get_collection(context)
        asset_name = context.asset_key.path[-1] if hasattr(context, 'asset_key') else context.asset_key_for_output(list(context.op_config.get("outs", {}).keys())[0]).path[-1] if context.op_config and context.op_config.get("outs") else "embedded_articles"
        records = obj.to_dict(orient="records")

        try:
            if asset_name in ["articles", "summarized_articles", "synced_articles"]:
                for record in records:
                    if "link" not in record:
                        context.log.error(f"Record missing 'link' field: {record}")
                        continue
                    if asset_name == "summarized_articles" and ("summary" not in record or not record["summary"]):
                        context.log.error(f"Record missing or empty 'summary' field: {record}")
                        continue
                    collection.update_one(
                        {"link": record["link"]},
                        {"$set": record},
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
            context.log.info(f"Loaded {len(docs)} documents for asset {context.asset_key.path[-1] if hasattr(context, 'asset_key') else context.asset_key_for_output(list(context.op_config.get('outs', {}).keys())[0]).path[-1] if context.op_config and context.op_config.get('outs') else 'embedded_articles'}")
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
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    def __del__(self):
        if hasattr(self, '_client') and self._client:
            self._client.close()

@io_manager
def mongo_io_manager():
    return MongoDBIOManager(
        config={
            "uri": os.getenv("MONGO_URI"),
            "database": os.getenv("MONGO_DB")
        }
    )