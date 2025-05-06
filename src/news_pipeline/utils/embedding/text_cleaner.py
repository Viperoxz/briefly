import re
from typing import List, Set

def clean_text(text: str) -> str:
    """Clean text by removing duplicates, noise, and normalizing."""
    # Remove photo credits (e.g., "Ảnh: Hoài Thanh")
    text = re.sub(r"Ảnh: [^\.]+", "", text)
    
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
    
    return ". ".join(unique_sentences) + (". " if unique_sentences else "")

def preprocess_texts(texts: List[str]) -> List[str]:
    """Preprocess multiple texts."""
    return [clean_text(text) for text in texts]