from dagster import (
    RunRequest,
    SkipReason,
    sensor,
    get_dagster_logger,
)
import os
from pymongo import MongoClient

from ..jobs.article_jobs import articles_processing_job
from ..utils.tts import initialize_auth_token, refresh_token_if_needed

@sensor(
    job=articles_processing_job,
    minimum_interval_seconds=120,
)
def article_partition_sensor(context):
    """Sensor to detect new articles and register them as partitions."""
    logger = get_dagster_logger()
    current_token = os.getenv("DAN_IO_AUTH_TOKEN")

    if current_token:
        refresh_token_if_needed(current_token, logger)
    else:
        initialize_auth_token(logger)
        
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]
    article_collection = db["articles"]

    # Get existing partitions
    existing_partitions = set(context.instance.get_dynamic_partitions("article_partitions"))

    # Find articles that have not been processed through the summary pipeline
    query = {"summary": {"$exists": False}}
    unsummarized_articles = list(article_collection.find(query, {"url": 1}).limit(20))
    tts_query = {
        "summary": {"$exists": True, "$ne": ""},
        "$or": [
            {"audio_url": {"$exists": False}},
            {"audio_url": ""}
        ]
    }
    articles_to_tts = list(article_collection.find(tts_query, {"url": 1}).limit(20))
    all_articles = unsummarized_articles + articles_to_tts
    
    if not all_articles:
        return SkipReason("No articles to process")

    # Register new partitions and prepare to run for existing partitions
    article_urls = [article["url"] for article in all_articles]
    new_urls = [url for url in article_urls if url not in existing_partitions]

    # Register new partitions
    if new_urls:
        context.instance.add_dynamic_partitions(
            "article_partitions",
            new_urls
        )
        logger.info(f"Registered {len(new_urls)} new partitions")

    # Create RunRequests for existing URLs
    existing_urls = [url for url in article_urls if url in existing_partitions]
    
    if not existing_urls:
        return SkipReason("No articles ready for processing")
    run_requests = []
    for url in existing_urls:
        run_requests.append(
            RunRequest(
                run_key=f"process_article_{url}",
                run_config={},
                tags={"article_url": url},
                partition_key=url
            )
        )
    return run_requests