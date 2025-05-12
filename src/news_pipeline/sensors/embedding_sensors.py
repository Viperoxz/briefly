from dagster import (
    DynamicPartitionsDefinition,
    get_dagster_logger,
    RunRequest,
    sensor,
    SkipReason
)
import os
from pymongo import MongoClient
from ..jobs.article_jobs import articles_embedding_job
import time
import random
import hashlib

@sensor(
    job=articles_embedding_job,
    minimum_interval_seconds=240, 
)
def embedding_partition_sensor(context):
    """Enhanced sensor to detect articles with summaries that need embeddings."""
    logger = get_dagster_logger()
    
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        article_collection = db["articles"]

        # Get existing partitions
        existing_partitions = set(context.instance.get_dynamic_partitions("article_partitions"))
        query = {
            "summary": {"$exists": True, "$ne": ""},
            "$or": [
                {"embedding_status": {"$exists": False}},
                {"embedding_status": None}
            ]
        }

        # Add logic to determine if backfill is needed
        current_time = time.time()
        last_backfill_key = "last_full_embedding_backfill"
        last_backfill = context.instance.get_sensor_cursor(context.sensor_name, last_backfill_key)
        backfill_interval = 3600 * 6  # 6 hours
        
        perform_backfill = False
        if not last_backfill or float(last_backfill) + backfill_interval < current_time:
            perform_backfill = True
        
        # Get articles based on backfill status
        articles_to_embed = []
        if perform_backfill:
            logger.info("Performing full backfill scan for missed articles")
            # Take all articles that have a summary but no embedding, no limit
            articles_to_embed = list(article_collection.find(query, {"url": 1}))
        else:
            articles_to_embed = list(article_collection.find(query, {"url": 1, "published_date": 1})
                                .sort("published_date", -1)  # Prioritize newer articles
                                .limit(10))
        
        if not articles_to_embed:
            return SkipReason("No articles ready for embedding")

        # Register new partitions
        article_urls = [article["url"] for article in articles_to_embed]
        new_urls = [url for url in article_urls if url not in existing_partitions]

        if new_urls:
            batch_size = 100
            for i in range(0, len(new_urls), batch_size):
                batch_urls = new_urls[i:i+batch_size]
                context.instance.add_dynamic_partitions("article_partitions", batch_urls)
                logger.info(f"Registered batch of {len(batch_urls)} new partitions for embedding")
        
        # Update backfill cursor only after successful processing
        if perform_backfill:
            context.instance.update_sensor_cursor(context.sensor_name, last_backfill_key, str(current_time))
            logger.info(f"Backfill: Processing {len(article_urls)} articles for embedding")
            # Select a random subset of URLs to process each time
            article_urls_to_process = random.sample(article_urls, min(15, len(article_urls)))
            
            run_requests = []
            for url in article_urls_to_process:
                run_requests.append(
                    RunRequest(
                        run_key=f"embed_article_{hashlib.md5(url.encode()).hexdigest()}_{int(current_time)}",
                        run_config={},
                        tags={"article_url": url, "process_type": "embedding", "source": "backfill"},
                        partition_key=url
                    )
                )
            return run_requests
        else:
            urls_to_process = article_urls[:10]

            run_requests = []
            for url in urls_to_process:
                run_requests.append(
                    RunRequest(
                        run_key=f"embed_article_{hashlib.md5(url.encode()).hexdigest()}_{int(current_time)}",
                        run_config={},
                        tags={"article_url": url, "process_type": "embedding", "source": "regular"},
                        partition_key=url
                    )
                )
            return run_requests
    except Exception as e:
        logger.error(f"Error in embedding sensor: {str(e)}")
        return SkipReason(f"Error in sensor: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()