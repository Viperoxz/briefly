from dagster import (
    Definitions,
    DynamicPartitionsDefinition,
    ScheduleDefinition,
    define_asset_job,
    SensorDefinition,
    RunRequest,
    SkipReason,
    AssetSelection,
    multiprocess_executor
)
from .assets.rss_feeds import rss_feed_list
from .assets.sources_and_topics import sources, topics
from .assets.raw_articles import raw_articles
from .assets.summarized_articles import summarized_articles
from .assets.embedded_articles import embedded_articles
from .assets.summary_for_audio import summary_for_audio  
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os
from dotenv import load_dotenv

load_dotenv()

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

MAX_CONCURRENCIES = int(os.getenv("MAX_CONCURRENCIES", 4))

articles_update_job = define_asset_job(
    name="articles_update_job",
    selection=AssetSelection.keys("rss_feed_list", "sources", "topics", "articles"),
    config={"ops": {"articles": {"config": {"save_json": False}}}}
)

summary_job = define_asset_job(
    name="summary_job",
    selection=AssetSelection.keys("articles", "summarized_articles", "summary_for_audio", "embedded_articles")
)


# Schedules
articles_update_schedule = ScheduleDefinition(
    job=articles_update_job,
    cron_schedule="*/3 * * * *",
    run_config={"ops": {"articles": {"config": {"save_json": False}}}}
)

summary_schedule = ScheduleDefinition(
    job=summary_job,
    cron_schedule="*/10 * * * *",  
    run_config={}
)

defs = Definitions(
    assets=[
        rss_feed_list,
        sources,
        topics,
        raw_articles,
        summarized_articles,
        summary_for_audio, 
        embedded_articles,
    ],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
    jobs=[articles_update_job, summary_job],
    schedules=[articles_update_schedule, summary_schedule],
    executor=multiprocess_executor.configured(
        {"max_concurrent": MAX_CONCURRENCIES}
    )
)