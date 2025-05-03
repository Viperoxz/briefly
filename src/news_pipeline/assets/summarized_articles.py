import pandas as pd
from dagster import asset, get_dagster_logger, Output
from ..utils.summarize_utils import summarize_content
from ..models.summarized_article import SummarizedArticle
from typing import List


@asset(
    key="summarized_articles",
    io_manager_key="mongo_io_manager",
    deps=["articles"]
)
def summarized_articles(articles: pd.DataFrame) -> Output[pd.DataFrame]:
    """Summarize articles, update summary in MongoDB, and store embeddings in Qdrant."""
    logger = get_dagster_logger()
    summarized_articles: List[dict] = []
    
    for _, article in articles.iterrows():
        try:
            # Validate article using Pydantic
            article_model = SummarizedArticle(
                source=article["source"],
                topic=article["topic"],
                title=article["title"],
                link=article["link"],
                image=article["image"],
                published=article["published"],
                content=article["content"],
                summary=""  # Temporary empty summary
            )
            
            # Summarize content
            summary = summarize_content(article_model.content)
            if not summary:
                logger.warning(f"Skipped summarization for {article_model.link}: No summary generated")
                continue
                
            article_model.summary = summary
            
            # # Generate embedding for summary
            # embedding = generate_embedding(summary)
            # if embedding is None:
            #     logger.warning(f"Skipped embedding for {article_model.link}: Failed to generate embedding")
            #     continue
                
            # # Store embedding in Qdrant
            # qdrant_io_manager.store_embedding(
            #     point_id=str(article_model.link),
            #     vector=embedding,
            #     payload={
            #         "source": article_model.source,
            #         "topic": article_model.topic,
            #         "title": article_model.title
            #     }
            # )
            
            summarized_articles.append(article_model.dict())
            
        except Exception as e:
            logger.error(f"Failed to process article {article.get('link', 'unknown')}: {e}")
            continue
    
    logger.info(f"Processed {len(summarized_articles)} summarized articles")
    
    # Convert to DataFrame for MongoDB storage
    df = pd.DataFrame(summarized_articles)
    
    return Output(
        value=df,
        metadata={
            "num_summarized_articles": len(summarized_articles),
            "sample_summary": summarized_articles[0]["summary"] if summarized_articles else None,
        }
    )