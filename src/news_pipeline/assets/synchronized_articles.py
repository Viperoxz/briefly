from dagster import multi_asset, AssetOut, Output, get_dagster_logger, DynamicPartitionsDefinition
import pandas as pd
from typing import List
from ..utils.summarization import summarize_content
from ..utils.embedding import generate_embedding
from ..models.summarized_article import SummarizedArticle
from ..resources.mongo_io_manager import MongoDBIOManager
from ..resources.qdrant_io_manager import QdrantIOManager

@multi_asset(
    outs={
        "synced_articles": AssetOut(io_manager_key="mongo_io_manager"),
        "summarized_articles": AssetOut(io_manager_key="mongo_io_manager"),
        "qdrant_embeddings": AssetOut(io_manager_key="qdrant_io_manager"),
    },
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    deps=["raw_articles"],
)
def synchronized_articles(context, raw_articles: pd.DataFrame, mongo_io_manager: MongoDBIOManager, qdrant_io_manager: QdrantIOManager):
    logger = get_dagster_logger()
    partition_key = context.partition_key  # Article link as partition key
    logger.info(f"Processing partition: {partition_key}")

    # Filter articles for the current partition (link)
    article = raw_articles[raw_articles["link"] == partition_key]
    if article.empty:
        logger.warning(f"No article found for partition {partition_key}")
        return {
            "synced_articles": Output(value=pd.DataFrame(), metadata={"status": "skipped"}),
            "summarized_articles": Output(value=pd.DataFrame(), metadata={"status": "skipped"}),
            "qdrant_embeddings": Output(value=None, metadata={"status": "skipped"}),
        }

    article = article.iloc[0]
    summarized_articles: List[dict] = []
    embeddings_stored = 0

    try:
        # Validate article
        article_model = SummarizedArticle(
            source=article["source"],
            topic=article["topic"],
            title=article["title"],
            link=article["link"],
            image=article["image"],
            published=article["published"],
            content=article["content"],
            summary=""
        )

        # Check if summary exists in MongoDB
        existing_article = mongo_io_manager._get_collection(context).find_one({"link": article_model.link})
        if existing_article and existing_article.get("summary"):
            logger.info(f"Summary already exists for {article_model.link}")
            article_model.summary = existing_article["summary"]
        else:
            # Generate summary
            summary = summarize_content(article_model.content)
            if not summary:
                logger.warning(f"Failed to generate summary for {article_model.link}")
                return {
                    "synced_articles": Output(value=pd.DataFrame([article]), metadata={"status": "no_summary"}),
                    "summarized_articles": Output(value=pd.DataFrame(), metadata={"status": "no_summary"}),
                    "qdrant_embeddings": Output(value=None, metadata={"status": "no_summary"}),
                }
            article_model.summary = summary

        # Generate embedding for summary
        embedding = generate_embedding(article_model.summary)
        if embedding is None:
            logger.warning(f"Failed to generate embedding for {article_model.link}")
            return {
                "synced_articles": Output(value=pd.DataFrame([article]), metadata={"status": "no_embedding"}),
                "summarized_articles": Output(value=pd.DataFrame([article_model.dict()]), metadata={"status": "no_embedding"}),
                "qdrant_embeddings": Output(value=None, metadata={"status": "no_embedding"}),
            }

        # Store embedding in Qdrant
        qdrant_io_manager.store_embedding(
            point_id=str(article_model.link),
            vector=embedding,
            payload={
                "source": article_model.source,
                "topic": article_model.topic,
                "title": article_model.title,
            }
        )
        embeddings_stored += 1

        summarized_articles.append(article_model.dict())

    except Exception as e:
        logger.error(f"Failed to process article {article.get('link', 'unknown')}: {e}")
        return {
            "synced_articles": Output(value=pd.DataFrame([article]), metadata={"status": "error"}),
            "summarized_articles": Output(value=pd.DataFrame(), metadata={"status": "error"}),
            "qdrant_embeddings": Output(value=None, metadata={"status": "error"}),
        }

    # Sync check: Remove orphaned Qdrant embeddings
    qdrant_points = qdrant_io_manager.load_input(context)
    mongo_links = set(raw_articles["link"])
    for point in qdrant_points:
        if point.id not in mongo_links:
            qdrant_io_manager.client.delete(
                collection_name=qdrant_io_manager.collection_name,
                points_selector=[point.id]
            )
            logger.info(f"Deleted orphaned Qdrant embedding: {point.id}")

    logger.info(f"Processed {len(summarized_articles)} summarized articles, stored {embeddings_stored} embeddings")

    return {
        "synced_articles": Output(
            value=pd.DataFrame([article]),
            metadata={"num_articles": 1}
        ),
        "summarized_articles": Output(
            value=pd.DataFrame(summarized_articles),
            metadata={
                "num_summarized_articles": len(summarized_articles),
                "sample_summary": summarized_articles[0]["summary"] if summarized_articles else None,
            }
        ),
        "qdrant_embeddings": Output(
            value=None,  # No direct output, handled by io_manager
            metadata={"num_embeddings": embeddings_stored}
        ),
    }