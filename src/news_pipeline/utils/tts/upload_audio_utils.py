import os
import time
import requests


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