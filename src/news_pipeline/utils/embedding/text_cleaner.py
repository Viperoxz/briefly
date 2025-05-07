import re
from typing import List, Set
from dagster import get_dagster_logger
import time

logger = get_dagster_logger()

def clean_text(text: str) -> str:
    """Clean text by removing duplicates, noise, and normalizing for Vietnamese processing."""
    start_time = time.time()
    
    # Remove photo credits (e.g., "Ảnh: Hoài Thanh")
    text = re.sub(r"Ảnh: [^\.]+", "", text)
    
    # Remove special characters that may interfere with Pyvi
    text = re.sub(r"[^\w\s.!?]", " ", text)
    
    # Normalize spaces and punctuation
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\.+", ".", text)
    
    # Remove duplicate sentences
    sentences = text.split(". ")
    seen: Set[str] = set()
    unique_sentences: List[str] = []
    
    for sentence in sentences:
        if sentence and sentence not in seen:
            seen.add(sentence)
            unique_sentences.append(sentence)
    
    cleaned_text = ". ".join(unique_sentences) + (". " if unique_sentences else "")
    logger.debug(f"Text cleaning took {time.time() - start_time:.2f}s")
    
    return cleaned_text

def preprocess_texts(texts: List[str]) -> List[str]:
    """Preprocess multiple texts."""
    start_time = time.time()
    cleaned_texts = [clean_text(text) for text in texts]
    logger.debug(f"Preprocessed {len(texts)} texts in {time.time() - start_time:.2f}s")
    return cleaned_texts