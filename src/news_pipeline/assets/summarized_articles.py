from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
from groq import Groq
from bson import ObjectId
from datetime import datetime
from ..resources.qdrant_io_manager import QdrantIOManager
from ..models import SummarizedArticle

@asset(
    key="summarized_articles",
    io_manager_key="mongo_io_manager",
    ins={"articles": AssetIn(key="articles")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions")
)
def summarized_articles(context, articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    if articles.empty:
        logger.warning("No articles to summarize")
        return Output(value=pd.DataFrame(), metadata={"num_summarized": 0})

    summarized_data = []
    failed_summaries = 0
    partition_key = context.partition_key

    for _, row in articles[articles["url"] == partition_key].iterrows():
        try:
            content = row["content"]
            response = client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "Summarize the following news article into 4 concise points in Vietnamese. Each point should be a short or medium sentence. Do not include any other information."
                    },
                    {"role": "user", "content": content}
                ],
                model=os.getenv("GROQ_MODEL_ID"),
                temperature=0.7,
                max_tokens=150
            )
            summary = response.choices[0].message.content.strip()
            if not summary or len(summary) < 1:
                logger.error(f"Unsuccessful summary for article{row['url']}")
                failed_summaries += 1
                continue

            summarized_article = SummarizedArticle(
                url=row["url"],
                title=row["title"],
                published_date=row["published_date"],
                source_id=row["source_id"],
                topic_id=row["topic_id"],
                summary=summary
            )
            summarized_data.append(summarized_article.model_dump())
            logger.info(f"✅ Summarized article: {row['url']}")
        except ValueError as ve:
            logger.error(f"❌ Value error for {row['url']}: {ve}")
            failed_summaries += 1
            continue
        except Exception as e:
            logger.error(f"❌ Failed to summarize {row['url']}: {e}")
            failed_summaries += 1
            continue

    df = pd.DataFrame(summarized_data)
    return Output(
        value=df,
        metadata={"num_summarized": len(df)}
    )