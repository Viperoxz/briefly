import os
from openai import OpenAI
import tempfile
import uuid
from dotenv import load_dotenv
import random

load_dotenv()

def generate_audio_file(text_content: str, format: str, gender: str):
    """Generate audio file using OpenAI TTS API."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as tmp_file:
        temp_path = tmp_file.name
    
    with client.audio.speech.with_streaming_response.create(
        model=os.getenv("OPENAI_MODEL_TTS_ID"),
        voice=select_voice(gender),
        input=text_content,
        response_format=format,
        instructions="""
        You are a Vietnamese news presenter. Speak fluently in Vietnamese, with a professional and expressive tone, similar to a news anchor. 
        Begin with a natural Vietnamese introduction such as "Thưa quý vị và các bạn" or "Theo thông tin chúng tôi nhận được". 
        When encountering English words, names, or abbreviations, pronounce them naturally in English. Read numbers, dates, and measurements in a way that sounds natural in Vietnamese. 
        Maintain a fast pace typical of news broadcasts, but ensure clarity and articulation.
        """
    ) as response:
        response.stream_to_file(temp_path)
    
    with open(temp_path, "rb") as f:
        audio_data = f.read()
    
    os.unlink(temp_path)
    return audio_data

def save_to_temp_file(audio_data: bytes, format: str) -> str:
    """Save audio data to a temporary file and return the path."""
    temp_filename = f"audio_{uuid.uuid4()}.{format}"
    temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
    
    with open(temp_path, 'wb') as f:
        f.write(audio_data)
    
    return temp_path

def select_voice(gender: str) -> str:
    """Select the voice for TTS."""
    male_voices = ['alloy', 'echo', 'fable', 'onyx']
    female_voices = ['shimmer', 'coral', 'nova', 'sage']
    if gender == 'male':
        return random.choice(male_voices)
    else:
        return random.choice(female_voices)