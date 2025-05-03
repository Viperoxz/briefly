from dagster import Definitions
from .assets.rss_feeds import rss_feed_list
from .assets.sources_and_topics import sources, topics
from .assets.raw_articles import raw_articles
from .assets.summarized_articles import summarized_articles
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

defs = Definitions(
    assets=[rss_feed_list, sources, topics, raw_articles, summarized_articles],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
)