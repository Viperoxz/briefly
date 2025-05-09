from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
from groq import Groq
from ..utils.summarization import summarize_content
from ..models import SummarizedArticle
from tenacity import retry, stop_after_attempt, wait_fixed

@asset(
    key="audio_summaries",
    io_manager_key="mongo_io_manager",
    ins={"summarized_articles": AssetIn(key="summarized_articles")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions")
)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def summary_for_audio(context, summarized_articles: pd.DataFrame) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    if summarized_articles.empty:
        logger.warning("No summarized articles to generate audio summaries for")
        return Output(value=pd.DataFrame(), metadata={"num_audio_summaries": 0})

    audio_summaries = []
    failed_summaries = 0
    partition_key = context.partition_key

    # Filter by partition key
    articles_to_process = summarized_articles[summarized_articles["url"] == partition_key]
    
    for _, row in articles_to_process.iterrows():
        try:
            # Use the existing summary as input instead of full content
            initial_summary = row["summary"]
            
            # Audio-specific prompt
            response = client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "Rephrase the following summary into a more conversational, audio-friendly format in Vietnamese. Keep it concise and clear for spoken delivery. Maintain the key points but make it sound natural when read aloud."
                    },
                    {"role": "user", "content": initial_summary}
                ],
                model=os.getenv("GROQ_MODEL_ID"),
                temperature=0.7,
                max_tokens=200
            )
            audio_summary = response.choices[0].message.content.strip()
            
            if not audio_summary or len(audio_summary) < 1:
                logger.error(f"Unsuccessful audio summary for article {row['url']}")
                failed_summaries += 1
                continue

            summarized_article = SummarizedArticle(
                url=row["url"],
                title=row["title"],
                published_date=row["published_date"],
                source_id=row["source_id"],
                topic_id=row["topic_id"],
                summary=audio_summary,  
            )
            audio_summaries.append(summarized_article.model_dump())
            logger.info(f"✅ Generated audio-friendly summary for article: {row['url']}")
        except ValueError as ve:
            logger.error(f"❌ Value error for {row['url']}: {ve}")
            failed_summaries += 1
            continue
        except Exception as e:
            logger.error(f"❌ Failed to generate audio summary for {row['url']}: {e}")
            failed_summaries += 1
            continue

    df = pd.DataFrame(audio_summaries)
    return Output(
        value=df,
        metadata={"num_audio_summaries": len(df), "failed_summaries": failed_summaries}
    )