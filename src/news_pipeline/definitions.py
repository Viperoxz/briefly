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
)
from .assets.rss_feeds import rss_feed_list
from .assets.sources_and_topics import sources, topics
from .assets.raw_articles import raw_articles
from .assets.summarized_articles import summarized_articles
from .assets.embedded_articles import embedded_articles
from .assets.synced_articles import synced_articles
from .assets.checked_summaries import checked_summaries
from .resources.mongo_io_manager import MongoDBIOManager
from .resources.qdrant_io_manager import QdrantIOManager
import os

# Define dynamic partitions for articles
article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

# Define jobs
raw_articles_job = define_asset_job(
    name="raw_articles_job",
    selection=["rss_feed_list", "sources", "topics", "articles"],  # Bao gồm tất cả upstream assets
)

sync_job = define_asset_job(
    name="sync_job",
    selection=["summarized_articles", "embedded_articles", "synced_articles", "qdrant_embeddings"],
    partitions_def=article_partitions_def
)

check_summary_job = define_asset_job(
    name="check_summary_job",
    selection="checked_summaries"
)

# Sensor to detect new articles and materialize all partitions
@sensor(job=sync_job, minimum_interval_seconds=60)  # Kiểm tra mỗi 1 phút
def article_sensor(context):
    # Lấy danh sách partition hiện tại
    existing_partitions = context.instance.get_dynamic_partitions("article_partitions")

    # Lấy danh sách bài báo từ MongoDB với collection cố định "Articles"
    mongo_io_manager = MongoDBIOManager(MONGO_CONFIG)
    collection = mongo_io_manager._get_collection(context, collection_name="Articles")
    articles = collection.find({}, {"link": 1})
    new_links = set(article["link"] for article in articles if article.get("link"))

    if not new_links:
        context.log.info("Không tìm thấy bài báo nào trong MongoDB")
        return SkipReason("No articles found in MongoDB")

    # Thêm partition mới
    partitions_to_add = new_links - set(existing_partitions)
    for partition in partitions_to_add:
        context.instance.add_dynamic_partitions("article_partitions", [partition])
        context.log.info(f"Added partition: {partition}")

    # Xóa partition không còn tồn tại
    partitions_to_remove = set(existing_partitions) - new_links
    for partition in partitions_to_remove:
        context.instance.delete_dynamic_partition("article_partitions", partition)
        context.log.info(f"Removed partition: {partition}")

    # Lấy danh sách partition hiện tại sau khi cập nhật
    updated_partitions = context.instance.get_dynamic_partitions("article_partitions")

    # Materialize tất cả partition nếu có
    if updated_partitions:
        for partition in updated_partitions:
            yield RunRequest(
                run_key=f"sync_job_{partition}",
                partition_key=partition
            )
    else:
        yield SkipReason("No partitions to materialize")

# Schedules
raw_articles_schedule = ScheduleDefinition(
    job=raw_articles_job,
    cron_schedule="*/5 * * * *",  # Chạy mỗi 3 phút
    run_config={}
)

check_summary_schedule = ScheduleDefinition(
    job=check_summary_job,
    cron_schedule="55 */6 * * *",  # Every 6 hours
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
        synced_articles,
        checked_summaries
    ],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG),
    },
    sensors=[article_sensor],
    schedules=[raw_articles_schedule, check_summary_schedule]
)