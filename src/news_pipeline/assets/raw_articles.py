import json
import html
from datetime import datetime
from pathlib import Path
import pandas as pd
import feedparser
from dagster import asset, get_dagster_logger, Output
from ..utils.extract_utils import (
    extract_full_article,
    extract_image_url_from_description,
)
from ..models.article import Article
from dateutil.parser import parse as parse_date


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

            for entry in feed.entries[:1]:  # Still limiting to 1 for now
                try:
                    title = html.unescape(html.unescape(entry.title))
                    content = extract_full_article(entry.link)
                    image_url = extract_image_url_from_description(entry.description)

                    if content and image_url:
                        article = Article(
                            source=source,
                            topic=topic,
                            title=title,
                            link=entry.link,
                            image=image_url,
                            published=parse_date(entry.get("published", "")).isoformat() if entry.get("published") else "",
                            content=content
                        )
                        articles.append(article.dict())
                        # articles.append({
                        #     "source": source,
                        #     "topic": topic,
                        #     "title": title,
                        #     "link": entry.link,
                        #     "image": image_url,
                        #     "published": entry.get("published", ""),
                        #     "content": content
                        # })
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
    raw_articles_dir = Path("data/raw")
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