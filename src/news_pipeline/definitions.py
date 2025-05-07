from dagster import Definitions, DynamicPartitionsDefinition, ScheduleDefinition, define_asset_job
from .assets.rss_feeds import rss_feed_list
from .assets.sources_and_topics import sources, topics
from .assets.raw_articles import raw_articles
from .assets.synchronized_articles import synchronized_articles
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os

# Define dynamic partitions for articles (partitioned by article link)
article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

# Define a job to materialize synchronized_articles
sync_job = define_asset_job(
    name="sync_job",
    selection="synchronized_articles",
    partitions_def=article_partitions_def
)

# Schedule to run every hour
sync_schedule = ScheduleDefinition(
    job=sync_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
    run_config={
        "ops": {"synchronized_articles": {}}
    }
)

defs = Definitions(
    assets=[rss_feed_list, sources, topics, raw_articles, synchronized_articles],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
    schedules=[sync_schedule]
)