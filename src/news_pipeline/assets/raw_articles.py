import html
import os
import time
import asyncio
from typing import List, Dict, Any, Optional
import random

from dateutil.parser import parse as parse_date
import pandas as pd
import feedparser
from dagster import asset, get_dagster_logger, Output
from pymongo import MongoClient
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import http.client
import urllib.error
import socket
import requests.exceptions

from ..utils.extraction import (
    extract_full_article,
    extract_image_url_from_description, 
    slugify,
    parse_feed_with_retry
)
from ..models import Article


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type((
        http.client.RemoteDisconnected, 
        urllib.error.URLError, 
        socket.timeout,
        requests.exceptions.RequestException,
        ConnectionError,
        ConnectionResetError
    )),
)
def extract_full_article_with_retry(url: str) -> str:
    """Wrapper around extract_full_article with retry logic"""
    return extract_full_article(url)

def get_existing_urls(article_collection) -> set:
    """Get all existing article URLs from MongoDB."""
    return set(doc["url"] for doc in article_collection.find({}, {"url": 1}))

@asset(
    description="Fetch articles from RSS feeds and store raw data in S3.",
    key="raw_articles",
    io_manager_key="s3_io_manager",
    group_name="raw_data",
    kinds={"python", "s3", "pydantic"},
    required_resource_keys={"mongo_db"}
)
def raw_articles(context, rss_feed_list: dict) -> Output[pd.DataFrame]:
    """ Fetch articles from RSS feeds and store them in S."""
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

    all_feeds = []
    for source, topics in rss_feed_list.items():
        for topic, url in topics.items():
            all_feeds.append((source, topic, url))
    random.shuffle(all_feeds)

    for source, topic, url in all_feeds:
        logger.info(f"\n📥 Fetching: {source} | {topic}")
        try:
            time.sleep(request_delay + random.uniform(0, 1.0))
            feed = parse_feed_with_retry(url, logger)
            total_entries = len(feed.entries)
            logger.info(f"🔗 Found {total_entries} entries")

            success_count = 0
            failure_count = 0
            
            entries_to_process = feed.entries[:max_entries_per_feed]
            for entry in entries_to_process:
                try:
                    # Check if article already exists
                    article_url = entry.link
                    if article_url in existing_urls:
                        logger.info(f"⏭️ Skipped (already in MongoDB): {article_url}")
                        continue
                    time.sleep(request_delay)
                    
                    title = html.unescape(html.unescape(entry.title))
                    try:
                        content = extract_full_article_with_retry(entry.link)
                    except Exception as e:
                        logger.warning(f"💥 Failed to extract content from {entry.link}: {e}")
                        failure_count += 1
                        continue                       
                    image_url = extract_image_url_from_description(entry.description)

                    source_doc = source_collection.find_one({"name": source})
                    topic_doc = topic_collection.find_one({"name": topic})
                    source_id = source_doc["_id"] if source_doc else None
                    topic_id = topic_doc["_id"] if topic_doc else None

                    published_str = entry.get("published", "")
                    published_dt = parse_date(published_str) if published_str else None
                    alias_title = slugify(title)

                    if content and image_url:  
                        try:
                            article_data = {
                                "source_id": source_id,
                                "topic_id": topic_id,
                                "title": title,
                                "url": entry.link,
                                "image": image_url,
                                "published_date": published_dt,
                                "content": content,
                                "alias": alias_title
                            }
                            article_obj = Article(**article_data)
                            articles.append(article_data)                           
                            success_count += 1
                            logger.info(f"✅ Successfully processed: {entry.link}")
                        except Exception as e:
                            logger.warning(f"❌ Skipped (validation failed: {str(e)}): {entry.link}")
                            failure_count += 1
                    else:
                        logger.warning(f"❌ Skipped (missing content or image): {entry.link}")
                        failure_count += 1
                except Exception as e:
                    logger.warning(f"💥 Failed to process entry from {entry.link}: {e}")
                    failure_count += 1
        except Exception as e:
            logger.warning(f"💥 Failed to parse feed {url}: {e}")
            continue

        logger.info(f"✅ Success: {success_count} | ❌ Failures: {failure_count}")

    logger.info(f"\n📦 Total collected: {len(articles)} articles")
    df = pd.DataFrame(articles) if articles else pd.DataFrame()

    return Output(
        value=df,
        metadata={
            "num_articles": len(articles),
            "sources": list(rss_feed_list.keys()),
            "success_rate": f"{len(articles)}/{sum(1 for _ in all_feeds)}"
        }
    )