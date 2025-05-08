from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
from groq import Groq
from ..resources.qdrant_io_manager import QdrantIOManager

@asset(
    key="summarized_articles",
    io_manager_key="mongo_io_manager",
    ins={"articles": AssetIn(key="articles")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions")
)
def summarized_articles(context, articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    # Initialize Groq client without the model parameter
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    if articles.empty:
        logger.warning("No articles to summarize")
        return Output(value=pd.DataFrame(), metadata={"num_summarized": 0})

    summarized_data = []
    partition_key = context.partition_key

    for _, row in articles[articles["link"] == partition_key].iterrows():
        try:
            content = row["content"]
            response = client.chat.completions.create(
                messages=[{"role": "user", "content": f"Summarize this article: {content}"}],
                model=os.getenv("GROQ_MODEL_ID"),  # Move model parameter here
            )
            summary = response.choices[0].message.content
            summarized_data.append({
                "link": row["link"],
                "summary": summary,
                "source": row["source"],
                "topic": row["topic"],
                "title": row["title"],
                "published": row["published"],
                "image": row["image"]
            })
            logger.info(f"✅ Summarized article: {row['link']}")
        except Exception as e:
            logger.error(f"❌ Failed to summarize {row['link']}: {e}")
            summarized_data.append({
                "link": row["link"],
                "summary": None,
                "source": row["source"],
                "topic": row["topic"],
                "title": row["title"],
                "published": row["published"],
                "image": row["image"]
            })

    df = pd.DataFrame(summarized_data)
    return Output(
        value=df,
        metadata={"num_summarized": len(df)}
    )