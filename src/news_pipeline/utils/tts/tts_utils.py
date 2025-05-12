import os
from openai import OpenAI
import tempfile
import uuid

def generate_audio_file(text_content: str) -> bytes:
    """Generate audio file using OpenAI TTS API."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp_file:
        temp_path = tmp_file.name
    
    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="echo",
        input=text_content,
        instructions="""
        Speak in Vietnamese. However, if the input contains English words or abbreviations,
        pronounce them naturally in English. For numbers like monetary values or measurements,
        read them in a way that sounds natural in Vietnamese context. Read in quite fast speed but maintain clarity.
        """
    ) as response:
        response.stream_to_file(temp_path)
    
    with open(temp_path, "rb") as f:
        audio_data = f.read()
    
    os.unlink(temp_path)
    return audio_data

def save_to_temp_file(audio_data: bytes) -> str:
    """Save audio data to a temporary file and return the path."""
    temp_filename = f"audio_{uuid.uuid4()}.mp3"
    temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
    
    with open(temp_path, 'wb') as f:
        f.write(audio_data)
    
    return temp_path