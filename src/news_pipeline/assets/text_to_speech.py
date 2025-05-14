from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
import math
from dotenv import load_dotenv
from bson import ObjectId
from ..utils.tts import (generate_audio_file, 
                         save_to_temp_file, 
                         upload_to_danio)

load_dotenv(override=True)

@asset(
    description="Generate audio files from text summaries, upload to API and store the respective IDs.",
    key="text_to_speech",
    io_manager_key="mongo_io_manager",
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    kinds={"mongodb", "gcs", "openai"}
)
def text_to_speech(context) -> Output[pd.DataFrame]:
    """
    Generate audio files from text summaries with both male and female voices,
    upload to API and store the respective IDs.
    """
    logger = get_dagster_logger()
    partition_key = context.partition_key
    
    # Find in MongoDB 
    logger.info(f"Fetching article with summary directly from MongoDB")
    mongo_io_manager = context.resources.mongo_io_manager
    articles_collection = mongo_io_manager._get_collection(context, "articles")
    article = articles_collection.find_one({"url": partition_key})

    # Check if summary exists
    if not article or "summary" not in article or not article["summary"]:
        logger.warning(f"Article {partition_key} does not have a summary")
        return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    # Check if both audio_ids already exist
    both_exists = (
        ("male_audio_id" in article and article["male_audio_id"] and not (isinstance(article["male_audio_id"], float) and math.isnan(article["male_audio_id"]))) and
        ("female_audio_id" in article and article["female_audio_id"] and not (isinstance(article["female_audio_id"], float) and math.isnan(article["female_audio_id"])))
    )
    
    if both_exists:
        logger.info(f"Article {partition_key} already has both audio files: male={article['male_audio_id']}, female={article['female_audio_id']}")
        return Output(
            value=pd.DataFrame([{
                "url": article["url"],
                "male_audio_id": article["male_audio_id"],
                "female_audio_id": article["female_audio_id"]
            }]),
            metadata={"num_audio_files": 2, "status": "already_exists"}
        )
    
    upload_auth_token = os.getenv("DAN_IO_AUTH_TOKEN")
    results = []
    voice_configs = [
        {"field": "male_audio_id", "gender": "male"},
        {"field": "female_audio_id", "gender": "female"}
    ]
    
    try:
        # logger.info(f"Generating audio for article: {partition_key}")
        # if isinstance(article["summary"], list):
        #     summary_text = "\n".join(article["summary"])
        # else:
        #     summary_text = article["summary"]
        audio_ids = {}
        summary_text = None
        
        for config in voice_configs:
            field = config["field"]
            gender = config["gender"]
            
            # Skip if this voice already has an audio ID
            if field in article and article[field] and not (isinstance(article[field], float) and math.isnan(article[field])):
                logger.info(f"Article already has {gender} audio: {article[field]}")
                audio_ids[field] = article[field]
                continue

            if summary_text is None:
                logger.info(f"Preparing summary text for article: {partition_key}")
                if isinstance(article["summary"], list):
                    summary_text = "\n".join(article["summary"])
                else:
                    summary_text = article["summary"]
            
            logger.info(f"Generating {gender} audio")
            audio_data = generate_audio_file(summary_text, 'wav', gender)
            temp_file_path = save_to_temp_file(audio_data, format='wav')

            try:
                audio_id = upload_to_danio(temp_file_path, upload_auth_token, logger)
                if not audio_id:
                    logger.error(f"Failed to get {gender} audio ID from API for {partition_key}")
                    continue
                
                logger.info(f"Successfully uploaded {gender} audio with ID: {audio_id}")
                
                try:
                    audio_id_obj = ObjectId(audio_id)
                    articles_collection.update_one(
                        {"url": partition_key},
                        {"$set": {field: audio_id_obj}}
                    )
                except Exception as e:
                    logger.warning(f"Could not convert {gender} audio_id to ObjectId: {e}, storing as string")
                    articles_collection.update_one(
                        {"url": partition_key},
                        {"$set": {field: audio_id}}
                    )
                
                logger.info(f"✅ Updated article with {gender} audio ID: {audio_id}")
                audio_ids[field] = audio_id
                
            except Exception as upload_error:
                logger.error(f"❌ Failed to upload {gender} audio for {partition_key}: {upload_error}")
            finally:
                # Clean up temporary file
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        
        # Add successful results to our return dataframe
        if audio_ids:
            results.append({
                "url": article["url"],
                "male_audio_id": audio_ids.get("male_audio_id", None),
                "female_audio_id": audio_ids.get("female_audio_id", None)
            })
            
    except Exception as e:
        logger.error(f"❌ Error processing audio for {partition_key}: {e}")

    df = pd.DataFrame(results)
    return Output(
        value=df,
        metadata={"num_audio_files": len(df) * 2 if results and all(k in results[0] for k in ["male_audio_id", "female_audio_id"]) else len(df)}
    )