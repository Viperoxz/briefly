from dagster import Definitions
from .assets import (
    rss_feed_list,
    sources,
    topics,
    raw_articles as articles,
    summarized_articles,
    embedded_articles
)
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
from .config import settings

MONGO_CONFIG = {
    "mongo_uri": settings.MONGO_URI,
    "mongo_db": settings.MONGO_DB,
    "mongo_collection": "Articles"
}

QDRANT_CONFIG = {
    "QDRANT_URL": settings.QDRANT_URL,
    "QDRANT_API_KEY": settings.QDRANT_API_KEY,
    "QDRANT_COLLECTION": "news_articles"
}

defs = Definitions(
    assets=[
        rss_feed_list,
        sources,
        topics,
        articles,
        summarized_articles,
        embedded_articles
    ],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
)