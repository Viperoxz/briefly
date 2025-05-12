from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
import requests
import math
import time
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from dotenv import load_dotenv
from ..utils.tts import (generate_audio_file, 
                         save_to_temp_file, 
                         refresh_auth_token, 
                         refresh_token_if_needed)
from bson import ObjectId

load_dotenv(override=True)

@asset(
    key="text_to_speech",
    io_manager_key="mongo_io_manager",
    # ins={"articles_with_summary": AssetIn(key="articles_with_summary")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    kinds={"mongodb", "openai"}
)
def text_to_speech(context) -> Output[pd.DataFrame]:
    """Generate audio files from text summaries and upload to Dan.io API."""
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
    
    # Check if audio_id already exists
    if ("audio_id" in article and article["audio_id"] and not (isinstance(article["audio_id"], float) and math.isnan(article["audio_id"]))):
        logger.info(f"Article {partition_key} already has an audio file: {article['audio_id']}")
        return Output(
            value=pd.DataFrame([{
                "url": article["url"],
                "audio_id": article["audio_id"]
            }]),
            metadata={"num_audio_files": 1, "status": "already_exists"}
        )
    upload_auth_token = os.getenv("DAN_IO_AUTH_TOKEN")
    results = []

    try:
        logger.info(f"Generating audio for article: {partition_key}")
        if isinstance(article["summary"], list):
            summary_text = "\n".join(article["summary"])
        else:
            summary_text = article["summary"]
        audio_data = generate_audio_file(summary_text)
        temp_file_path = save_to_temp_file(audio_data)

        try:
            audio_id = upload_to_danio(temp_file_path, upload_auth_token, logger)
            if not audio_id:
                logger.error(f"Failed to get audio ID from Dan.io API for {partition_key}")
                return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
            logger.info(f"Successfully uploaded audio with ID: {audio_id}")

            mongo_io_manager = context.resources.mongo_io_manager
            articles_collection = mongo_io_manager._get_collection(context, "articles")
            try:
                audio_id_obj = ObjectId(audio_id)
                articles_collection.update_one(
                    {"url": partition_key},
                    {"$set": {"audio_id": audio_id_obj}}
                )
            except Exception as e:
                logger.warning(f"Could not convert audio_id to ObjectId: {e}, storing as string")
                articles_collection.update_one(
                    {"url": partition_key},
                    {"$set": {"audio_id": audio_id}}
                )
            logger.info(f"✅ Updated article with audio ID: {audio_id}")
            results.append({
                "url": article["url"],
                "audio_id": audio_id_obj if isinstance(audio_id_obj, ObjectId) else audio_id
            })
        except Exception as upload_error:
            logger.error(f"❌ Failed to upload audio for {partition_key}: {upload_error}")
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    except Exception as e:
        logger.error(f"❌ Error processing audio for {partition_key}: {e}")
    
    df = pd.DataFrame(results)
    return Output(
        value=df,
        metadata={"num_audio_files": len(df)}
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException) | retry_if_exception_type(ConnectionError)
)
def upload_to_danio(file_path: str, auth_token: str, logger) -> str:
    """Upload audio file to Dan.io API and return the file ID.
    
    Args:
        file_path: Path to the audio file
        auth_token: Authentication token
        logger: Logger instance
    
    Returns:
        str: Audio ID if upload successful, None otherwise
    """
    try:
        # Ensure we have a valid token
        current_token = refresh_token_if_needed(auth_token, logger)
        
        upload_url = 'https://api.dan.io.vn/media/api/v1/upload'
        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'access-control-allow-origin': '*',
            'authorization': f'Bearer {current_token}',
            'lang': 'en',
            'origin': 'https://api.dan.io.vn',
            'referer': 'https://api.dan.io.vn/media/swagger/index.html',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f, 'audio/mpeg')}
            logger.info(f"Uploading audio file: {os.path.basename(file_path)}")
            response = requests.post(upload_url, headers=headers, files=files, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('error_code') == 0 and 'data' in data and 'id' in data['data']:
                audio_id = data['data']['id']
                logger.info(f"Successfully uploaded audio with ID: {audio_id}")
                return audio_id
            else:
                logger.error(f"No ID in response or unexpected structure: {data}")
                return None
        elif response.status_code == 401:
            # Token expired - need to refresh and retry
            logger.warning("Token expired during upload, refreshing...")
            new_token = refresh_auth_token(logger)
            if new_token:
                # Save the new token in environment
                os.environ["DAN_IO_AUTH_TOKEN"] = new_token
                os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
                # Retry with new token (will be handled by the retry decorator)
                raise requests.exceptions.RequestException("Token expired, retrying with new token")
            else:
                logger.error("Failed to refresh token during upload")
                return None
        else:
            logger.error(f"API Error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise