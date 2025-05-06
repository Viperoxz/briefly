from dagster import Definitions
from .assets import rss_feed_list, sources, topics, raw_articles, summarized_articles, embedded_articles
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

defs = Definitions(
    assets=[rss_feed_list, sources, topics, raw_articles, summarized_articles, embedded_articles],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
)