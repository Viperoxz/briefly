from sentence_transformers import SentenceTransformer
from dagster import get_dagster_logger
from dotenv import load_dotenv
import os


load_dotenv()

# Initialize the model (loaded once globally)
embedding_model = os.getenv("EMBEDDING_MODEL_ID")
model = SentenceTransformer(embedding_model)

def generate_embedding(text: str) -> list:
    """Generate embedding for a given text using sentence-transformers."""
    logger = get_dagster_logger()
    try:
        embedding = model.encode(text, convert_to_tensor=False).tolist()
        logger.info(f"Generated embedding for text (length: {len(embedding)})")
        return embedding
    except Exception as e:
        logger.error(f"Failed to generate embedding: {e}")
        return None