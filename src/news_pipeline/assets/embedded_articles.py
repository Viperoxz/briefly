from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from tenacity import retry, stop_after_attempt, wait_fixed
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from ..models.article import Article
import pandas as pd
import numpy as np

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="embedded_articles",
    io_manager_key="mongo_io_manager",  
    partitions_def=article_partitions_def,
    ins={"summarized_articles": AssetIn(key="summarized_articles")}  
)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))  
def embedded_articles(context, raw_articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    partition_key = context.partition_key

    # Filter articles by partition_key
    articles = raw_articles[raw_articles['url'] == partition_key]
    if articles.empty:
        logger.error(f"No article found with url {partition_key}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

    if len(articles) > 1:
        logger.warning(f"Multiple articles found for url {partition_key}, using first match")
    article = articles.iloc[0]

    try:
        article_model = Article(**article.to_dict())

        cleaned_content = clean_text(article_model.content)
        chunks = chunk_text(cleaned_content)
        if not chunks:
            logger.warning(f"No chunks generated for {article_model.url}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

        # Generate embeddings for all chunks and average them
        embeddings_list = generate_embedding([chunks])[0]
        if not embeddings_list or not any(embeddings_list):
            logger.warning(f"No embeddings generated for {article_model.url}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})
        
        # Average embeddings across chunks
        embeddings = np.mean(embeddings_list, axis=0).tolist() if embeddings_list else None

        # Ánh xạ source_id và topic_id thành tên
        mongo_io_manager = context.resources.mongo_io_manager
        source_doc = mongo_io_manager._get_collection(context, "sources").find_one({"_id": article_model.source_id})
        topic_doc = mongo_io_manager._get_collection(context, "topics").find_one({"_id": article_model.topic_id})
        source_name = source_doc["name"] if source_doc else ""
        topic_name = topic_doc["name"] if topic_doc else ""

        # Store embedding in Qdrant with updated payload
        payload = {
            "title": article_model.title,
            "source_name": source_name,
            "topic_name": topic_name,
            "published_date": article_model.published_date.isoformat()
        }
        context.resources.qdrant_io_manager.store_embedding(
            point_id=str(article_model.url),
            vector=embeddings,
            payload=payload
        )

        logger.info(f"Generated embeddings for article {article_model.url}")
        embedded_article = EmbeddedArticle(**article_model.model_dump())
        output_data = embedded_article.model_dump()
        output_data["url"] = article_model.url
        return Output(
            value=pd.DataFrame([output_data]),
            metadata={"num_articles": 1}
        )
    except Exception as e:
        logger.error(f"Error processing article {partition_key}: {str(e)} - Article data: {article.to_dict()}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})