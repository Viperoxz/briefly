import pandas as pd
from dagster import asset, Output
from ..utils.extraction import slugify


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
        {"name": topic, "alias": slugify(topic)}
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
