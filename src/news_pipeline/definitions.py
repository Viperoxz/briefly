from dagster import (
    Definitions,
    DynamicPartitionsDefinition,
    ScheduleDefinition,
    define_asset_job,
    SensorDefinition,
    RunRequest,
    sensor,
    SkipReason,
    AssetSelection,
    multiprocess_executor
)
from .assets.rss_feeds import rss_feed_list
from .assets.sources_and_topics import sources, topics
from .assets.raw_articles import raw_articles
from .assets.summarized_articles import summarized_articles
from .assets.embedded_articles import embedded_articles
from .assets.sync_up import sync_up
from .assets.checked_summaries import checked_summaries
from .assets.clean_orphaned_embeddings import clean_orphaned_embeddings
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

raw_articles_job = define_asset_job(
    name="raw_articles_job",
    selection=["rss_feed_list", "sources", "topics", "articles"],
    config={"ops": {"raw_articles": {"config": {"save_json": False}}}}
)

sync_job = define_asset_job(
    name="sync_job",
    selection=AssetSelection.assets("summarized_articles", "embedded_articles", "synced_articles").required_multi_asset_neighbors(),
    partitions_def=article_partitions_def
)

check_summary_job = define_asset_job(
    name="check_summary_job",
    selection="checked_summaries"
)

clean_orphaned_job = define_asset_job(
    name="clean_orphaned_job",
    selection=AssetSelection.assets("synced_articles", "clean_orphaned_embeddings").required_multi_asset_neighbors()
)

@sensor(job=sync_job, minimum_interval_seconds=60)
def article_sensor(context):
    existing_partitions = context.instance.get_dynamic_partitions("article_partitions")
    mongo_io_manager = MongoDBIOManager(MONGO_CONFIG)
    collection = mongo_io_manager._get_collection(context, collection_name="articles")
    articles = collection.find({}, {"link": 1})
    new_links = set(article["link"] for article in articles if article.get("link"))

    if not new_links:
        return SkipReason("No articles found in MongoDB")

    MAX_PARTITIONS_PER_RUN = 7
    partitions_to_add = list(new_links - set(existing_partitions))[:MAX_PARTITIONS_PER_RUN]
    for partition in partitions_to_add:
        context.instance.add_dynamic_partitions("article_partitions", [partition])
        context.log.info(f"Added partition: {partition}")

    partitions_to_remove = set(existing_partitions) - new_links
    for partition in partitions_to_remove:
        context.instance.delete_dynamic_partition("article_partitions", partition)
        context.log.info(f"Removed partition: {partition}")

    updated_partitions = context.instance.get_dynamic_partitions("article_partitions")
    if updated_partitions:
        for partition in updated_partitions:
            yield RunRequest(
                run_key=f"sync_job_{partition}",
                partition_key=partition
            )
    else:
        yield SkipReason("No partitions to materialize")

@sensor(job=sync_job, minimum_interval_seconds=60)
def sync_sensor(context):
    mongo_io_manager = MongoDBIOManager(MONGO_CONFIG)
    collection = mongo_io_manager._get_collection(context, collection_name="articles")
    articles = collection.find(
        {"summary": {"$exists": True}, "embeddings": {"$exists": True}},
        {"link": 1}
    )
    ready_links = set(article["link"] for article in articles)

    if not ready_links:
        return SkipReason("No articles ready for sync")

    existing_partitions = context.instance.get_dynamic_partitions("article_partitions")
    MAX_PARTITIONS_PER_RUN = 10
    partitions_to_sync = list(ready_links & set(existing_partitions))[:MAX_PARTITIONS_PER_RUN]

    for partition in partitions_to_sync:
        yield RunRequest(
            run_key=f"sync_job_{partition}",
            partition_key=partition
        )

@sensor(job=sync_job, minimum_interval_seconds=3600)  # Run every hour
def cleanup_sensor(context):
    mongo_io_manager = MongoDBIOManager(MONGO_CONFIG)
    collection = mongo_io_manager._get_collection(context, collection_name="synced_articles")
    result = collection.delete_many({"sync_status": "pending"})
    context.log.info(f"Deleted {result.deleted_count} pending records from synced_articles collection")

raw_articles_schedule = ScheduleDefinition(
    job=raw_articles_job,
    cron_schedule="*/3 * * * *",
    run_config={"ops": {"raw_articles": {"config": {"save_json": False}}}}
)

check_summary_schedule = ScheduleDefinition(
    job=check_summary_job,
    cron_schedule="55 */6 * * *",
    run_config={}
)

clean_orphaned_schedule = ScheduleDefinition(
    job=clean_orphaned_job,
    cron_schedule="0 */12 * * *",
    run_config={}
)

defs = Definitions(
    assets=[
        rss_feed_list,
        sources,
        topics,
        raw_articles,
        summarized_articles,
        embedded_articles,
        sync_up,
        checked_summaries,
        clean_orphaned_embeddings
    ],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
    sensors=[article_sensor, sync_sensor, cleanup_sensor],
    schedules=[raw_articles_schedule, check_summary_schedule, clean_orphaned_schedule],
    executor=multiprocess_executor.configured(
        {"max_concurrent": 4}
    )
)