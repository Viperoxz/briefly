from dagster import sensor, get_dagster_logger, SkipReason, RunRequest
from pymongo import MongoClient
from ..jobs.article_jobs import related_articles_job
import os
import hashlib
import time
import datetime
from datetime import timedelta

@sensor(
    job=related_articles_job,
    minimum_interval_seconds=300,  
)
def related_articles_sensor(context):
    """
    Sensor to detect articles that need related articles processing:
    1. New articles with summary and validation_score but no related_ids
    2. Articles with related_ids older than 1 day that need refreshing
    
    Triggers when at least 100 eligible articles exist.
    """
    logger = get_dagster_logger()
    
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        article_collection = db["articles"]

        existing_partitions = set(context.instance.get_dynamic_partitions("article_partitions"))
        current_time = datetime.datetime.now(datetime.timezone.utc)
        logger.info(f"Current time: {current_time}")

        refresh_threshold = current_time - timedelta(days=1)

        query = {
            "$and": [
                {"summary": {"$exists": True, "$ne": ""}},
                {"validation_score": {"$exists": True}},
                {"embedding_status": "completed"},
                {"$or": [
                    # Trường hợp 1: Chưa có related_ids
                    {"related_ids": {"$exists": False}},
                    {"related_ids": {"$size": 0}},
                    
                    # Trường hợp 2: Đã có related_ids nhưng cần làm mới
                    {
                        "related_ids": {"$exists": True},
                        "$or": [
                            {"related_ids_updated_at": {"$lt": refresh_threshold}},
                            {"related_ids_updated_at": {"$exists": False}}
                        ]
                    }
                ]}
            ]
        }
        
        eligible_count = article_collection.count_documents(query)
        if eligible_count < 100:
            return SkipReason(f"Only {eligible_count} articles eligible for related articles processing. Need at least 100.")

        last_processed = context.cursor
        
        # Backfill logic 
        backfill_interval = 3600 * 6  # 6 hours
        perform_backfill = False
        timestamp = int(time.time())
        
        if not last_processed or float(last_processed) + backfill_interval < timestamp:
            perform_backfill = True
            batch_size = 50  
        else:
            batch_size = 20  
        
        # Ưu tiên các bài viết chưa có related_ids
        priority_query = {
            "summary": {"$exists": True, "$ne": ""},
            "validation_score": {"$exists": True},
            "embedding_status": "completed",
            "$or": [
                {"related_ids": {"$exists": False}},
                {"related_ids": {"$size": 0}}
            ]
        }
        
        priority_articles = list(article_collection.find(priority_query, {"url": 1}).limit(batch_size))
        
        # Nếu chưa đủ batch_size, lấy thêm các bài cần làm mới
        remaining_slots = batch_size - len(priority_articles)
        
        refresh_articles = []
        if remaining_slots > 0:
            refresh_query = {
                "summary": {"$exists": True, "$ne": ""},
                "validation_score": {"$exists": True},
                "embedding_status": "completed",
                "related_ids": {"$exists": True, "$ne": []},
                "$or": [
                    {"related_ids_updated_at": {"$lt": refresh_threshold}},
                    {"related_ids_updated_at": {"$exists": False}}
                ]
            }
            refresh_articles = list(article_collection.find(refresh_query, {"url": 1}).limit(remaining_slots))
        
        eligible_articles = priority_articles + refresh_articles
        
        if not eligible_articles:
            return SkipReason("No articles available for related articles processing")

        article_urls = [article["url"] for article in eligible_articles]
        new_urls = [url for url in article_urls if url not in existing_partitions]
        
        if new_urls:
            context.instance.add_dynamic_partitions("article_partitions", new_urls)
            logger.info(f"Registered {len(new_urls)} new partitions for related articles processing")
        
        # Tạo run requests
        run_requests = []
        for url in article_urls:
            # Xác định loại xử lý (mới hoặc refresh)
            is_refresh = any(art.get("url") == url for art in refresh_articles)
            
            run_requests.append(
                RunRequest(
                    run_key=f"related_articles_{hashlib.md5(url.encode()).hexdigest()}_{timestamp}",
                    run_config={},
                    tags={
                        "article_url": url,
                        "process_type": "related_articles",
                        "operation": "refresh" if is_refresh else "new",
                        "source": "backfill" if perform_backfill else "regular"
                    },
                    partition_key=url,
                    cursor=str(timestamp) if perform_backfill else None
                )
            )
        
        logger.info(f"Created {len(run_requests)} run requests for related articles processing " +
                    f"({len(priority_articles)} new, {len(refresh_articles)} refreshes)")
        return run_requests
        
    except Exception as e:
        logger.error(f"Error in related_articles sensor: {str(e)}")
        return SkipReason(f"Error in sensor: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()