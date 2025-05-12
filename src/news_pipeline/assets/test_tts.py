from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
import time
from dotenv import load_dotenv
from ..utils.tts import generate_audio_file, save_to_temp_file
from pathlib import Path
import re

load_dotenv(override=True)

@asset(
    key="test_tts",
    io_manager_key="mongo_io_manager",
    ins={"articles_with_summary": AssetIn(key="articles_with_summary")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions")
)
def test_tts(context, articles_with_summary) -> Output[pd.DataFrame]:
    """
    Generate audio files from text summaries and save to local filesystem.
    This is a test version that doesn't upload to Dan.io API.
    """
    logger = get_dagster_logger()
    audio_storage_dir = os.getenv("TEST_AUDIO_DIR", os.path.join(os.getcwd(), "test_audio"))
    Path(audio_storage_dir).mkdir(parents=True, exist_ok=True)
    partition_key = context.partition_key

    filtered_articles = articles_with_summary[articles_with_summary["url"] == partition_key] if not articles_with_summary.empty else pd.DataFrame()
    if filtered_articles.empty:
        logger.info(f"Fetching article directly from MongoDB as it was not found in the input DataFrame")
        mongo_io_manager = context.resources.mongo_io_manager
        articles_collection = mongo_io_manager._get_collection(context, "articles")
        article = articles_collection.find_one({"url": partition_key})
        
        if article and "summary" in article and article["summary"]:
            filtered_articles = pd.DataFrame([article])
            logger.info(f"Successfully retrieved article with summary from MongoDB")
        else:
            logger.warning(f"No summarized article found for partition {partition_key}")
            return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    if filtered_articles.empty:
        logger.warning(f"No summarized article found for partition {partition_key}")
        return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    # filtered_articles = articles_with_summary[articles_with_summary["url"] == partition_key]
    
    # if filtered_articles.empty:
    #     logger.warning(f"No article found with partition key {partition_key} in summarized articles")
    #     return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    article_row = filtered_articles.iloc[0].to_dict()
    
    # Kiểm tra summary
    if "summary" not in article_row or not article_row["summary"]:
        logger.warning(f"Article {partition_key} does not have a summary")
        return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    # Kiểm tra nếu đã có audio_url
    if "audio_url" in article_row and article_row["audio_url"]:
        logger.info(f"Article {partition_key} already has an audio file: {article_row['audio_url']}")
        return Output(
            value=pd.DataFrame([{
                "url": article_row["url"],
                "audio_url": article_row["audio_url"]
            }]),
            metadata={"num_audio_files": 1, "status": "already_exists"}
        )
    
    results = []

    try:
        logger.info(f"Generating audio for article: {partition_key}")
        audio_data = generate_audio_file(article_row["summary"])

        # Generate a unique filename
        timestamp = int(time.time())
        safe_id = re.sub(r'[^\w\-_]', '_', str(partition_key))  
        audio_filename = f"article_{safe_id}_{timestamp}.mp3"
        audio_filepath = os.path.join(audio_storage_dir, audio_filename)
        
        # Save the file
        with open(audio_filepath, "wb") as f:
            f.write(audio_data)
        
        logger.info(f"Saved audio file to: {audio_filepath}")
        
        # Update MongoDB with the audio URL
        audio_url = f"file://{audio_filepath}"
        mongo_io_manager = context.resources.mongo_io_manager
        articles_collection = mongo_io_manager._get_collection(context, "articles")
        articles_collection.update_one(
            {"url": partition_key},
            {"$set": {"audio_url": audio_url}}
        )
        logger.info(f"✅ Updated article with audio URL: {audio_url}")
        
        results.append({
            "url": article_row["url"],
            "audio_url": audio_url,
            "audio_filepath": audio_filepath
        })
        
    except Exception as e:
        logger.error(f"❌ Error processing audio for {article_row['url']}: {e}")

    df = pd.DataFrame(results)
    return Output(
        value=df,
        metadata={"num_audio_files": len(df)}
    )