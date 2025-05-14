from dagster import sensor, get_dagster_logger, SkipReason, RunRequest
from pymongo import MongoClient
from ..jobs import articles_tts_job
import os
import random
import time
import hashlib

@sensor(
    job=articles_tts_job,
    minimum_interval_seconds=60,  
)
def tts_partition_sensor(context):
    """Sensor to detect articles with summaries that need TTS."""
    logger = get_dagster_logger()
    
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        article_collection = db["articles"]

        existing_partitions = set(context.instance.get_dynamic_partitions("article_partitions"))

        # Find articles with summaries but no audio
        # Trong hàm tts_partition_sensor
        query = {
            "summary": {"$exists": True, "$ne": ""},
            "$or": [
                {"male_audio_id": {"$exists": False}},
                {"female_audio_id": {"$exists": False}}
            ]
        }

        # Add logic to determine if backfill is needed
        current_time = time.time()
        backfill_interval = 3600 * 6  # 6 hours

        # Lấy trạng thái cursor hiện tại hoặc dùng None nếu chưa có 
        last_backfill = context.cursor
        perform_backfill = False
        if not last_backfill or float(last_backfill) + backfill_interval < current_time:
            perform_backfill = True
        
        # Get articles based on backfill status
        articles_to_process = []
        if perform_backfill:
            logger.info("Performing full backfill scan for articles missing TTS")
            # Get more articles during backfill mode
            articles_to_process = list(article_collection.find(query, {"url": 1}))
        else:
            articles_to_process = list(article_collection.find(query, {"url": 1, "published_date": 1})
                                    .sort("published_date", -1)  # Prioritize newer articles
                                    .limit(30))
        
        if not articles_to_process:
            return SkipReason("No articles ready for TTS processing")
        
        article_urls = [article['url'] for article in articles_to_process]
        new_urls = [url for url in article_urls if url not in existing_partitions]
        
        if new_urls:
            batch_size = 100
            for i in range(0, len(new_urls), batch_size):
                batch_urls = new_urls[i:i+batch_size]
                context.instance.add_dynamic_partitions("article_partitions", batch_urls)
                logger.info(f"Registered batch of {len(batch_urls)} new partitions for TTS")
        
        # Process the articles
        if perform_backfill:
            # Đã xóa dòng cập nhật cursor kiểu cũ
            # context.instance.update_url(context.sensor_name, last_backfill_key, str(current_time))
            logger.info(f"Backfill: Processing {len(article_urls)} articles for TTS")
            # Select a random subset of URLs to process each time
            article_urls_to_process = random.sample(article_urls, min(30, len(article_urls)))
            
            run_requests = []
            for url in article_urls_to_process:
                run_requests.append(
                    RunRequest(
                        run_key=f"tts_{hashlib.md5(url.encode()).hexdigest()}_{int(current_time)}",
                        run_config={},
                        tags={"article_url": url, "process_type": "tts", "source": "backfill"},
                        partition_key=url,
                        cursor=str(current_time)  # Đặt cursor trực tiếp vào RunRequest
                    )
                )
            return run_requests
        else:
            # Limit the number of articles to process in each run to avoid overload
            urls_to_process = article_urls[:20]  # Max 20 articles per run
            
            run_requests = []
            for url in urls_to_process:
                run_requests.append(
                    RunRequest(
                        run_key=f"tts_{hashlib.md5(url.encode()).hexdigest()}_{int(current_time)}",
                        run_config={},
                        tags={"article_url": url, "process_type": "tts", "source": "regular"},
                        partition_key=url
                    )
                )
            # Không cần cập nhật cursor khi không phải backfill
            return run_requests
    except Exception as e:
        logger.error(f"Error in TTS sensor: {str(e)}")
        return SkipReason(f"Error in sensor: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()