from langchain.text_splitter import (
    RecursiveCharacterTextSplitter,
    SentenceTransformersTokenTextSplitter,
)
from ...config import settings
from typing import List
from transformers import AutoTokenizer

def chunk_text(text: str) -> List[str]:
    """Split text into chunks optimized for Vietnamese news articles."""
    # Step 1: Split by characters
    character_splitter = RecursiveCharacterTextSplitter(
        separators=[". ", "!", "?"],
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP_CHAR,
        keep_separator=True,
    )
    text_splits = character_splitter.split_text(text)

    tokenizer = AutoTokenizer.from_pretrained(
        settings.EMBEDDING_MODEL_ID,
        trust_remote_code=True
    )


    # Step 2: Split by tokens
    token_splitter = SentenceTransformersTokenTextSplitter(
        tokenizer=tokenizer,
        chunk_overlap=settings.CHUNK_OVERLAP_TOKEN,
        tokens_per_chunk=settings.EMBEDDING_MODEL_MAX_INPUT_LENGTH,
    )
    chunks = []

    for section in text_splits:
        chunks.extend(token_splitter.split_text(section))

    # Remove very short chunks
    return [chunk for chunk in chunks if len(chunk) > 10]

def chunk_multiple_texts(texts: List[str]) -> List[List[str]]:
    """Chunk multiple texts."""
    return [chunk_text(text) for text in texts]