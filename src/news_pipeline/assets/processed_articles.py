from dagster import asset, get_dagster_logger, Output, AssetIn
import pandas as pd
from ..models.raw_article import RawArticle
from ..models.processed_article import ProcessedArticle
from ..utils.text_processing import clean_article_content
import re
from datetime import datetime

@asset(
    description="Clean and process raw articles data, convert to parquet format.",
    key="processed_articles",
    io_manager_key="s3_parquet_io_manager",
    ins={"raw_articles": AssetIn(key="raw_articles")},
    group_name="processing",
    kinds={"python", "s3", "pydantic", "pandas"},
)
def processed_articles(context, raw_articles) -> Output[pd.DataFrame]:
    """Clean and process raw articles, transform to a structured format for analytics."""
    logger = get_dagster_logger()
    
    if raw_articles.empty:
        logger.info("No raw articles to process")
        return Output(
            value=pd.DataFrame(),
            metadata={"num_articles": 0, "status": "empty_input"}
        )
    
    processed_data = []
    
    for _, row in raw_articles.iterrows():
        try:
            # Create a RawArticle object and validate the input
            raw_article = RawArticle(**row.to_dict())
            
            # Clean the content
            cleaned_content = clean_article_content(raw_article.content)
            
            # Create processed article
            processed_article = ProcessedArticle.from_raw_article(
                raw_article=raw_article,
                cleaned_content=cleaned_content
            )
            
            processed_data.append(processed_article.model_dump())
            
        except Exception as e:
            logger.error(f"Error processing article {row.get('url')}: {str(e)}")
    
    if not processed_data:
        return Output(
            value=pd.DataFrame(),
            metadata={"num_articles": 0, "status": "all_failed"}
        )
    
    df = pd.DataFrame(processed_data)
    
    return Output(
        value=df,
        metadata={
            "num_articles": len(df),
            "content_length_avg": df["content_length"].mean() if "content_length" in df.columns else 0,
            "process_timestamp": datetime.now().isoformat()
        }
    )