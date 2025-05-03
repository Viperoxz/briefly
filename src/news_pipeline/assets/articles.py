import os

from dagster import asset, get_dagster_logger, Definitions, Output
import json
import feedparser
import html
from typing import Dict
import pandas as pd
from datetime import datetime
from pathlib import Path
from ..resources.mongo_io_manager import MongoDBIOManager
from ..utils.extract_utils import (
    extract_full_article,
    extract_image_url_from_description,
    slugify,
    alias_from_topic
)
from dotenv import load_dotenv

load_dotenv()


@asset
def rss_feed_list() -> Dict:
    """Load RSS feed list from JSON config file."""
    # file_path = "/home/duckthihn/PycharmProjects/new_pipeline/news_pipeline/data/rss_feeds.json"
    base_dir = Path(__file__).parent.parent.parent
    file_path = base_dir / "data" / "rss_feeds.json"
    
    with open(file_path, "r", encoding="utf-8") as f:
        rss_sources = json.load(f)
    return rss_sources


@asset(
    key="sources",
    io_manager_key="mongo_io_manager"
)
def sources(rss_feed_list: dict) -> Output[pd.DataFrame]:
    """Get the source and alias name."""
    source_info = [
        {"name": source, "alias": slugify(source)}
        for source in rss_feed_list.keys()
    ]

    # Convert to DataFrame for MongoDB storage
    df = pd.DataFrame(source_info)

    return Output(
        value=df,
        metadata={
            "source_count": len(source_info),
            "sample_source": source_info[3] if source_info else None,
        }
    )


@asset(
    key="topics",
    io_manager_key="mongo_io_manager"
)
def topics(rss_feed_list: dict) -> Output[pd.DataFrame]:
    """Get the topic and alias name."""
    topic_set = set()
    for source in rss_feed_list.values():
        topic_set.update(source.keys())

    topics = [
        {"name": topic, "alias": alias_from_topic(topic)}
        for topic in sorted(topic_set)
    ]

    # Convert to DataFrame for MongoDB storage
    df = pd.DataFrame(topics)

    return Output(
        value=df,
        metadata={
            "topic_count": len(topics),
            "sample_topic": topics[:3] if topics else None,
        }
    )


@asset(
    key="articles",
    io_manager_key="mongo_io_manager"
)
def raw_articles(rss_feed_list: dict) -> Output[pd.DataFrame]:
    """Fetch raw articles from all RSS feeds and save to MongoDB."""
    logger = get_dagster_logger()
    articles = []

    for source, topics in rss_feed_list.items():
        for topic, url in topics.items():
            logger.info(f"\n📥 Fetching: {source} | {topic}")
            feed = feedparser.parse(url)
            total_entries = len(feed.entries)
            logger.info(f"🔗 Found {total_entries} entries")

            success_count = 0
            failure_count = 0

            for entry in feed.entries[:1]:  # still limiting to 1 for now
                try:
                    title = html.unescape(html.unescape(entry.title))
                    content = extract_full_article(entry.link)
                    image_url = extract_image_url_from_description(entry.description)

                    if content and image_url:
                        articles.append({
                            "source": source,
                            "topic": topic,
                            "title": title,
                            "link": entry.link,
                            "image": image_url,
                            "published": entry.get("published", ""),
                            "content": content
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

    # Save articles to JSON file
    raw_articles_dir = Path("../data/raw_articles")
    raw_articles_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = raw_articles_dir / f"articles_{timestamp}.json"

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    logger.info(f"💾 Saved articles to {json_path}")

    # Convert to DataFrame for MongoDB storage
    df = pd.DataFrame(articles)

    return Output(
        value=df,
        metadata={
            "num_articles": len(articles),
            "sample_articles": articles[0] if articles else None,
            "sources": list(rss_feed_list.keys()),
            "json_path": str(json_path)
        }
    )


MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

defs = Definitions(
    assets=[rss_feed_list, sources, topics, raw_articles],
    resources={
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG)
    },
)
