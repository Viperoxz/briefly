from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import asyncio
from ..utils.summarization import process_articles_async, fact_check_article
from bson import ObjectId


@asset(
    description="Summarize articles and store the summaries in MongoDB.",
    key="articles_with_summary",
    io_manager_key="mongo_io_manager",
    ins={"raw_articles": AssetIn(key="raw_articles")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    kinds={"mongodb", "openai"}
)
def articles_with_summary(context, raw_articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()

    # if articles.empty:
    #     logger.warning("No articles to summarize")
    #     return Output(value=pd.DataFrame(), metadata={"num_summarized": 0})

    partition_key = context.partition_key
    mongo_io_manager = context.resources.mongo_io_manager
    collection = mongo_io_manager._get_collection(context, "articles")
    article = collection.find_one({"url": partition_key})
    
    if not article:
        logger.warning(f"No article found for partition {partition_key}")
        return Output(value=pd.DataFrame(), metadata={"num_summarized": 0})

    article_df = pd.DataFrame([article])
    result_df = asyncio.run(process_articles_async(article_df, context.resources.mongo_io_manager))

    successful_summaries = result_df['summary'].count()
    successful_summaries = int(successful_summaries)
    failed_summaries = int(len(result_df) - successful_summaries)

    return Output(
        value=result_df,
        metadata={
            "num_summarized": successful_summaries,
            "failed_summaries": failed_summaries
        }
    )