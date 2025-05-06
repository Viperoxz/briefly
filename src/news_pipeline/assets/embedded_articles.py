import pandas as pd
from dagster import asset, get_dagster_logger, Output
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from typing import List, Dict

@asset(
    key="embedded_articles",
    io_manager_key="qdrant_io_manager",
    deps=["summarized_articles"]
)
def embedded_articles(articles: pd.DataFrame) -> Output[List[Dict]]:
    """Clean, chunk, embed articles, and store embeddings in Qdrant."""
    logger = get_dagster_logger()
    embedded_articles: List[Dict] = []
    
    # Prepare texts for embedding
    texts = []
    article_data = []
    for _, article in articles.iterrows():
        try:
            # Validate article using Pydantic
            article_model = EmbeddedArticle(
                source=article["source"],
                topic=article["topic"],
                title=article["title"],
                link=article["link"],
                image=article.get("image", None),
                published=article["published"],
                content=article["content"],
                summary=article.get("summary", ""),
                embeddings=[]
            )
            
            # Clean and chunk content
            cleaned_content = clean_text(article_model.content)
            chunks = chunk_text(cleaned_content)
            if not chunks:
                logger.warning(f"No chunks generated for {article_model.link}")
                continue
                
            texts.append(chunks)
            article_data.append(article_model)
        except Exception as e:
            logger.error(f"Failed to process article {article.get('link', 'unknown')}: {e}")
            continue
    
    # Generate embeddings
    embeddings_list = generate_embedding(texts)
    
    # Store embeddings and prepare output
    for article_model, chunks, embeddings in zip(article_data, texts, embeddings_list):
        try:
            article_model.embeddings = embeddings
            embedded_articles.append(article_model.dict())
            logger.info(f"Embedded {len(chunks)} chunks for {article_model.link}")
        except Exception as e:
            logger.error(f"Failed to store embeddings for {article_model.link}: {e}")
            continue
    
    logger.info(f"Processed {len(embedded_articles)} embedded articles")
    
    return Output(
        value=embedded_articles,
        metadata={
            "num_embedded_articles": len(embedded_articles),
            "sample_embedding": embedded_articles[0]["embeddings"][0][:5] if embedded_articles and embedded_articles[0]["embeddings"] else None,
        }
    )