import html
import os
import time
import random
import pandas as pd
from dagster import asset, get_dagster_logger, Output, AssetIn
from dateutil.parser import parse as parse_date
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import requests.exceptions
from typing import Dict, Tuple, Optional

from ..utils.extraction import (
    extract_full_article,
    extract_image_url_from_description,
    parse_feed_with_retry,
)
from ..models import RawArticle

# Define retry configuration for network-related operations
RETRY_CONFIG = {
    "stop": stop_after_attempt(3),
    "wait": wait_fixed(2),
    "retry": retry_if_exception_type((
        requests.exceptions.RequestException,
        ConnectionError,
        ConnectionResetError,
    )),
}

@retry(**RETRY_CONFIG)
def extract_full_article_with_retry(url: str) -> str:
    """Retry wrapper for extracting full article content with error handling."""
    return extract_full_article(url)

def get_existing_urls(article_collection) -> set:
    """Retrieve a set of existing article URLs from the MongoDB articles collection."""
    return {doc["url"] for doc in article_collection.find({}, {"url": 1})}

def process_feed_entry(
    entry,
    source_id: str,
    topic_id: str,
    existing_urls: set,
    logger,
    request_delay: float,
) -> Optional[dict]:
    """Process a single RSS feed entry into a RawArticle dictionary."""
    try:
        article_url = entry.link
        if article_url in existing_urls:
            logger.info(f"⏭️ Skipped (already in MongoDB): {article_url}")
            return None

        time.sleep(request_delay)
        title = html.unescape(entry.title)
        content = extract_full_article_with_retry(article_url)
        image_url = extract_image_url_from_description(entry.description)

        if not (content and image_url):
            logger.warning(f"❌ Skipped (missing content or image): {article_url}")
            return None

        published_str = entry.get("published", "")
        published_dt = parse_date(published_str) if published_str else None

        raw_article = RawArticle(
            source_id=source_id,
            topic_id=topic_id,
            title=title,
            url=article_url,
            image=image_url,
            published_date=published_dt,
            content=content,
        )
        logger.info(f"✅ Successfully processed: {article_url}")
        return raw_article.model_dump()

    except Exception as e:
        logger.warning(f"💥 Failed to process entry {article_url}: {str(e)}")
        return None

def process_rss_feed(
    source: str,
    topic: str,
    url: str,
    source_collection,
    topic_collection,
    existing_urls: set,
    logger,
    max_entries_per_feed: int,
    request_delay: float,
) -> Tuple[int, int, list]:
    """Process a single RSS feed and return success and failure counts."""
    try:
        time.sleep(request_delay + random.uniform(0, 1.0))
        feed = parse_feed_with_retry(url, logger)
        logger.info(f"🔗 Found {len(feed.entries)} entries in {url}")

        source_doc = source_collection.find_one({"name": source})
        topic_doc = topic_collection.find_one({"name": topic})
        source_id = str(source_doc["_id"]) if source_doc else None
        topic_id = str(topic_doc["_id"]) if topic_doc else None

        if not (source_id and topic_id):
            logger.warning(f"💥 Missing source or topic ID for {source}/{topic}")
            return 0, len(feed.entries), []

        success_count = 0
        failure_count = 0
        entries_to_process = feed.entries[:max_entries_per_feed]
        articles = []
        

        for entry in entries_to_process:
            result = process_feed_entry(entry, source_id, topic_id, existing_urls, logger, request_delay)
            if result:
                success_count += 1
                articles.append(result)
            else:
                failure_count += 1

        logger.info(f"✅ Success: {success_count} | ❌ Failures: {failure_count} for {url}")
        return success_count, failure_count, articles

    except Exception as e:
        logger.warning(f"💥 Failed to parse feed {url}: {str(e)}")
        return 0, len(feed.entries) if "feed" in locals() else 0, []

@asset(
    description="Fetch articles from RSS feeds and store raw data in S3.",
    key="raw_articles",
    io_manager_key="s3_io_manager",
    group_name="raw_data",
    kinds={"python", "s3", "pydantic"},
    required_resource_keys={"mongo_db"},
)
def raw_articles(context, rss_feed_list: Dict[str, Dict[str, str]]) -> Output[pd.DataFrame]:
    """Fetch articles from RSS feeds, validate with RawArticle model, and store in S3."""
    logger = get_dagster_logger()
    articles = []

    db = context.resources.mongo_db
    source_collection = db["sources"]
    topic_collection = db["topics"]
    article_collection = db["articles"]

    existing_urls = get_existing_urls(article_collection)
    logger.info(f"📊 Found {len(existing_urls)} existing articles in MongoDB")

    max_entries_per_feed = int(os.getenv("MAX_ENTRIES_PER_FEED", 1))
    request_delay = float(os.getenv("REQUEST_DELAY", 1.0))

    all_feeds = [(source, topic, url) for source, topics in rss_feed_list.items() for topic, url in topics.items()]
    random.shuffle(all_feeds)

    total_success = 0
    total_failures = 0
    for source, topic, url in all_feeds:
        success, failures, feed_articles = process_rss_feed(
            source, topic, url, source_collection, topic_collection, existing_urls, logger,
            max_entries_per_feed, request_delay
        )
        total_success += success
        total_failures += failures
        articles.extend(feed_articles)

    logger.info(f"\n📦 Total collected: {total_success} articles, {total_failures} failures")
    df = pd.DataFrame(articles) if articles else pd.DataFrame()
    
    s3_key = f"raw_data/raw_articles_{context.run_id}.json"
    return Output(
        value=df,
        metadata={
            "num_articles": total_success,
            "sources": list(rss_feed_list.keys()),
            "success_rate": f"{total_success}/{len(all_feeds)}",
            "total_entries_processed": total_success + total_failures,
            "s3_key": s3_key
        }
    )