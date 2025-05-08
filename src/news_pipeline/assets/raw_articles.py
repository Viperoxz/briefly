import html
import os

from dateutil.parser import parse as parse_date
import pandas as pd
import feedparser
from dagster import asset, get_dagster_logger, Output
from pymongo import MongoClient

from ..utils.extraction import (
    extract_full_article,
    extract_image_url_from_description, slugify,
)


@asset(
    key="articles",
    io_manager_key="mongo_io_manager"
)
def raw_articles(rss_feed_list: dict) -> Output[pd.DataFrame]:
    """ Fetch articles from RSS feeds and store them in MongoDB."""
    logger = get_dagster_logger()
    articles = []

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    source_collection = db["sources"]
    topic_collection = db["topics"]
    article_collection = db["articles"]

    for source, topics in rss_feed_list.items():
        for topic, url in topics.items():
            logger.info(f"\n📥 Fetching: {source} | {topic}")
            feed = feedparser.parse(url)
            total_entries = len(feed.entries)
            logger.info(f"🔗 Found {total_entries} entries")

            success_count = 0
            failure_count = 0

            for entry in feed.entries[:100]:
                try:
                    # Check if article already exists
                    article_url = entry.link
                    if article_collection.find_one({"url": article_url}):
                        logger.info(f"⏭️ Skipped (already in MongoDB): {article_url}")
                        continue

                    title = html.unescape(html.unescape(entry.title))
                    content = extract_full_article(entry.link)
                    image_url = extract_image_url_from_description(entry.description)

                    source_doc = source_collection.find_one({"name": source})
                    topic_doc = topic_collection.find_one({"name": topic})
                    source_id = source_doc["_id"] if source_doc else None
                    topic_id = topic_doc["_id"] if topic_doc else None

                    published_str = entry.get("published", "")
                    published_dt = parse_date(published_str) if published_str else None
                    alias_title = slugify(title)

                    if content and image_url:
                        articles.append({
                            "source_id": source_id,
                            "topic_id": topic_id,
                            "title": title,
                            "url": entry.link,
                            "image": image_url,
                            "published_date": published_dt,
                            "content": content,
                            "alias": alias_title
                        })
                        success_count += 1
                    else:
                        logger.warning(f"❌ Skipped (missing content or image): {entry.link}")
                        failure_count += 1
                except Exception as e:
                    logger.warning(f"💥 Failed to extract from {entry.link}: {e}")
                    failure_count += 1

            logger.info(f"✅ Success: {success_count} | ❌ Failures: {failure_count}")

    logger.info(f"\n📦 Total collected: {len(articles)} articles")
    df = pd.DataFrame(articles)

    return Output(
        value=df,
        metadata={
            "num_articles": len(articles),
            "sources": list(rss_feed_list.keys())
        }
    )
