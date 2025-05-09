from dagster import multi_asset, AssetOut, Output, get_dagster_logger, DynamicPartitionsDefinition, AssetIn
import pandas as pd
from ..models.summarized_article import SummarizedArticle

@multi_asset(
    outs={
        "synced_articles": AssetOut(io_manager_key="mongo_io_manager"),
        "qdrant_embeddings": AssetOut(io_manager_key="qdrant_io_manager"),
    },
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    ins={
        "summarized_articles": AssetIn(key="summarized_articles"),
        "embedded_articles": AssetIn(key="embedded_articles"),
    },
)
def sync_up(context, summarized_articles: pd.DataFrame, embedded_articles: pd.DataFrame):
    logger = get_dagster_logger()
    partition_key = context.partition_key

    # Check for 'url' column in summarized_articles
    if "url" not in summarized_articles.columns:
        logger.error(f"❌ Missing 'url' column in summarized_articles for partition {partition_key}")
        return (
            Output(value=pd.DataFrame(), metadata={"num_synced": 0}),
            Output(value=pd.DataFrame(), metadata={"num_embeddings": 0})
        )
    
    # Check for 'url' column in embedded_articles
    if "url" not in embedded_articles.columns:
        logger.error(f"❌ Missing 'url' column in embedded_articles for partition {partition_key}")
        return (
            Output(value=pd.DataFrame(), metadata={"num_synced": 0}),
            Output(value=pd.DataFrame(), metadata={"num_embeddings": 0})
        )

    summarized_article = summarized_articles[summarized_articles["url"] == partition_key]
    has_summary = not summarized_article.empty and "summary" in summarized_article.iloc[0] and summarized_article.iloc[0]["summary"]

    embedded_article = embedded_articles[embedded_articles["url"] == partition_key]
    has_embedding = not embedded_article.empty and "embeddings" in embedded_article.iloc[0] and embedded_article.iloc[0]["embeddings"]

    if not has_summary or not has_embedding:
        logger.warning(f"Article {partition_key} not ready for sync")
        context.resources.mongo_io_manager.collection.update_one(
            {"url": partition_key},
            {"$set": {"sync_status": "pending"}},
            upsert=True
        )
        return [
            Output(
                value=pd.DataFrame(),
                output_name="synced_articles",
                metadata={"status": "pending"}
            ),
            Output(
                value=None,
                output_name="qdrant_embeddings",
                metadata={"status": "pending"}
            ),
        ]

    article_dict = summarized_article.iloc[0].to_dict()
    article_model = SummarizedArticle(**article_dict)

    context.resources.mongo_io_manager.collection.update_one(
        {"url": article_model.url},
        {"$set": {"sync_status": "synced"}},
        upsert=True
    )

    logger.info(f"Synced article {partition_key}")
    return [
        Output(
            value=pd.DataFrame([article_model.dict()]),
            output_name="synced_articles",
            metadata={"num_articles": 1}
        ),
        Output(
            value=None,
            output_name="qdrant_embeddings",
            metadata={"num_synced": 1}
        ),
    ]