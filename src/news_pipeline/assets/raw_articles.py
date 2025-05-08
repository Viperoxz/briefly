import json
import html
from datetime import datetime
from pathlib import Path
import pandas as pd
import feedparser
from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from ..utils.extraction import (
    extract_full_article,
    extract_image_url_from_description
)
from ..models.article import Article
from dateutil.parser import parse as parse_date
import hashlib

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="articles",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    ins={
        "rss_feed_list": AssetIn(key="rss_feed_list"),
        "sources": AssetIn(key="sources"),
        "topics": AssetIn(key="topics")
    }
)
def raw_articles(context, rss_feed_list: dict, sources: pd.DataFrame, topics: pd.DataFrame) -> Output[pd.DataFrame]:
    """Fetch raw articles from all RSS feeds and save to MongoDB."""
    logger = get_dagster_logger()
    articles = []
    processed_links = set()

    # Convert sources and topics to dictionaries for lookup
    valid_sources = set(sources['name'].tolist()) if not sources.empty else set()
    valid_topics = set(topics['name'].tolist()) if not topics.empty else set()

    for source, topics_dict in rss_feed_list.items():
        # Validate source
        if valid_sources and source not in valid_sources:
            logger.warning(f"Skipping invalid source: {source}")
            continue

        for topic, url in topics_dict.items():
            # Validate topic
            if valid_topics and topic not in valid_topics:
                logger.warning(f"Skipping invalid topic: {topic} for source {source}")
                continue

            logger.info(f"\n📥 Fetching: {source} | {topic}")
            feed = feedparser.parse(url)
            total_entries = len(feed.entries)
            logger.info(f"🔗 Found {total_entries} entries")

            success_count = 0
            failure_count = 0

            for entry in feed.entries[:10]:  
                try:
                    link = entry.link
                    if link in processed_links:
                        logger.warning(f"🔄 Skipping duplicate link: {link}")
                        continue
                    processed_links.add(link)

                    title = html.unescape(entry.title) if entry.title else "No Title"
                    content = extract_full_article(link)
                    image_url = extract_image_url_from_description(entry.description)

                    if not content or not image_url:
                        logger.warning(f"❌ Skipped (missing content or image): {link}")
                        failure_count += 1
                        continue

                    # Generate name from title or link hash if not provided
                    name = title[:100] if title else hashlib.md5(link.encode()).hexdigest()[:100]

                    article = Article(
                        source=source,
                        topic=topic,
                        name=name,  # Ensure name is populated
                        title=title,
                        link=link,
                        image=image_url,
                        published=parse_date(entry.get("published", "")).isoformat() if entry.get("published") else datetime.now().isoformat(),
                        content=content
                    )
                    articles.append(article.model_dump())
                    success_count += 1

                    # Add partition for this link
                    context.instance.add_dynamic_partitions(article_partitions_def.name, [link])
                    logger.info(f"🎯 Added partition: {link}")

                except Exception as e:
                    logger.warning(f"💥 Failed to extract from {entry.link}: {e}")
                    failure_count += 1

            logger.info(f"✅ Success: {success_count} | ❌ Failures: {failure_count}")

    logger.info(f"\n📦 Total collected: {len(articles)} articles")

    # Optional JSON saving (check if op_config is None)
    save_json = context.op_config.get("save_json", False) if context.op_config is not None else False
    if save_json:
        raw_articles_dir = Path("data/raw")
        raw_articles_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = raw_articles_dir / f"articles_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Saved articles to {json_path}")
    else:
        json_path = "JSON saving disabled"

    # Convert to DataFrame for MongoDB storage
    df = pd.DataFrame(articles)

    # Thêm logging để kiểm tra DataFrame
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    if not df.empty:
        logger.info(f"Sample data: {df.iloc[0].to_dict()}")
    else:
        logger.warning("DataFrame is empty!")

    return Output(
        value=df,
        metadata={
            "num_articles": len(articles),
            "sample_articles": articles[0] if articles else None,
            "sources": list(rss_feed_list.keys()),
            "json_path": str(json_path)
        }
    )