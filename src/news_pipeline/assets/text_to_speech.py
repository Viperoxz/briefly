from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
import pandas as pd
import os
import requests
from bson import ObjectId
from datetime import datetime
import urllib.parse
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv

load_dotenv()

@asset(
    key="generated_audio",
    io_manager_key="mongo_io_manager",
    ins={"audio_summaries": AssetIn(key="audio_summaries")},
    partitions_def=DynamicPartitionsDefinition(name="article_partitions")
)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def text_to_speech(context, audio_summaries: pd.DataFrame) -> Output[pd.DataFrame]:
    """Generate audio files from text summaries using a TTS service."""
    logger = get_dagster_logger()
    
    if audio_summaries.empty:
        logger.warning("No audio summaries to process")
        return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    # Get the TTS API configuration from environment variables
    tts_api_key = os.getenv("TTS_API_KEY")
    tts_api_url = os.getenv("TTS_API_URL")
    audio_storage_path = os.getenv("AUDIO_STORAGE_PATH", "audio_files")
    audio_base_url = os.getenv("AUDIO_BASE_URL", "https://yourdomain.com/audio")
    
    # Create storage directory if it doesn't exist
    os.makedirs(audio_storage_path, exist_ok=True)
    
    # Filter by partition key
    partition_key = context.partition_key
    articles_to_process = audio_summaries[audio_summaries["url"] == partition_key]
    
    if articles_to_process.empty:
        logger.warning(f"No audio summary found for partition {partition_key}")
        return Output(value=pd.DataFrame(), metadata={"num_audio_files": 0})
    
    results = []
    for _, row in articles_to_process.iterrows():
        try:
            # Generate a unique filename based on article URL
            sanitized_title = urllib.parse.quote(row["title"][:30], safe='')
            filename = f"{sanitized_title}_{datetime.now().strftime('%Y%m%d%H%M%S')}.mp3"
            filepath = os.path.join(audio_storage_path, filename)
            
            # Get the text to convert to speech
            text_for_tts = row["summary"]
            
            # Call the TTS API (replace with your preferred TTS service)
            # Example using a REST API
            response = requests.post(
                tts_api_url,
                json={
                    "text": text_for_tts,
                    "voice": "vietnamese_female",  # Adjust based on your TTS provider
                    "output_format": "mp3"
                },
                headers={"Authorization": f"Bearer {tts_api_key}"},
            )
            
            if response.status_code == 200:
                # Save the audio file
                with open(filepath, "wb") as f:
                    f.write(response.content)
                
                # Generate the public URL
                audio_url = f"{audio_base_url}/{filename}"
                
                # Create result record
                result = {
                    "url": row["url"],
                    "title": row["title"],
                    "published_date": row["published_date"],
                    "source_id": row["source_id"],
                    "topic_id": row["topic_id"],
                    "summary_for_audio": text_for_tts,
                    "audio_url": audio_url
                }
                results.append(result)
                logger.info(f"✅ Generated audio for {row['url']}")
            else:
                logger.error(f"❌ Failed to generate audio for {row['url']}: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Error generating audio for {row['url']}: {e}")
    
    df = pd.DataFrame(results)
    return Output(
        value=df,
        metadata={"num_audio_files": len(df)}
    )