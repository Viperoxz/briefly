import os
import time
import requests
import time
import json
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from dotenv import load_dotenv
from .tts_utils import generate_audio_file, save_to_temp_file

load_dotenv(override=True)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException) | retry_if_exception_type(ConnectionError)
)
def upload_to_danio(file_path: str, auth_token: str, logger) -> str:
    """Upload audio file to API and return the file ID.
    
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
            files = {'file': (file_path, f, 'audio/mpeg')}
            # file_data = f.read()
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


def refresh_token_if_needed(auth_token: str, logger) -> None:
    """Check if token needs refresh based on last refresh time."""
    last_refresh_time = os.environ.get("DAN_IO_TOKEN_LAST_REFRESH")
    current_token = auth_token
    
    if not last_refresh_time or (time.time() - float(last_refresh_time)) > 14 * 60:  # 14 minutes
        logger.info("Token may be expired, refreshing...")
        new_token = refresh_auth_token(logger)
        if new_token:
            current_token = new_token
            os.environ["DAN_IO_AUTH_TOKEN"] = new_token
            os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
            logger.info("Token refreshed successfully")
        else:
            logger.error("Failed to refresh token")
    
    return current_token

def refresh_auth_token(logger) -> str:
    """Get a new authentication token."""
    try:
        email = os.getenv("AUDIO_UPLOAD_EMAIL")
        password = os.getenv("AUDIO_UPLOAD_PASSWORD")
        
        if not email or not password:
            logger.error("Lack of login information (AUDIO_UPLOAD_EMAIL or AUDIO_UPLOAD_PASSWORD)")
            return None
        
        login_url = 'https://api.dan.io.vn/api/v1/session/signin'
        payload = {'email': email, 'password': password}
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        response = requests.post(login_url, headers=headers, json=payload, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'access_token' in data['data']:
                logger.info("Successfully retrieved authentication token")
                return data['data']['access_token']
            else:
                logger.error(f"Unexpected response structure: {data}")
                return None
        else:
            logger.error(f"Auth Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error refreshing token: {e}")
        return None
    
def initialize_auth_token(logger) -> str:
    """Initialize auth token when application starts."""
    logger.info("Initializing Dan.io authentication token...")
    token = refresh_auth_token(logger)
    if token:
        os.environ["DAN_IO_AUTH_TOKEN"] = token
        os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
        logger.info("Successfully initialized authentication token")
        return token
    else:
        logger.error("Failed to initialize authentication token")
        return None