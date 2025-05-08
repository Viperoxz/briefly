from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from tenacity import retry, stop_after_attempt, wait_fixed
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from ..models.article import Article
import pandas as pd

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="embedded_articles",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    ins={"raw_articles": AssetIn(key="articles")}
)
def embedded_articles(context, raw_articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    partition_key = context.partition_key

    # Filter articles by partition_key
    articles = raw_articles[raw_articles['link'] == partition_key]
    if articles.empty:
        logger.error(f"No article found with link {partition_key}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

    if len(articles) > 1:
        logger.warning(f"Multiple articles found for link {partition_key}, using first match")
    article = articles.iloc[0]

    try:
        article_model = Article(**article.to_dict())

        cleaned_content = clean_text(article_model.content)
        chunks = chunk_text(cleaned_content)
        if not chunks:
            logger.warning(f"No chunks generated for {article_model.link}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

        # Generate embeddings for all chunks and average them
        embeddings_list = generate_embedding(chunks)
        if not embeddings_list or not any(embeddings_list):
            logger.warning(f"No embeddings generated for {article_model.link}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})
        
        # Average embeddings across chunks
        import numpy as np
        embeddings = np.mean(embeddings_list, axis=0).tolist() if embeddings_list else None

        # Store embedding in Qdrant
        context.resources.qdrant_io_manager.store_embedding(
            point_id=str(article_model.link),
            vector=embeddings,
            payload={
                "source": article_model.source,
                "topic": article_model.topic,
                "title": article_model.title,
            }
        )

        # Update MongoDB with embeddings
        context.resources.mongo_io_manager.collection.update_one(
            {"link": article_model.link},
            {"$set": {"embeddings": embeddings}},
            upsert=True
        )

        logger.info(f"Generated embeddings for article {article_model.link}")
        embedded_article = EmbeddedArticle(**article_model.dict(), embeddings=embeddings)
        # Ensure 'link' is explicitly included in the output DataFrame
        output_data = embedded_article.dict()
        output_data["link"] = article_model.link  # Explicitly add 'link' if not in dict()
        return Output(
            value=pd.DataFrame([output_data]),
            metadata={"num_articles": 1}
        )
    except Exception as e:
        logger.error(f"Error processing article {partition_key}: {str(e)} - Article data: {article.to_dict()}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})