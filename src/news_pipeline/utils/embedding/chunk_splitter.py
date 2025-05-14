import torch
from langchain.text_splitter import RecursiveCharacterTextSplitter
from typing import List
from transformers import AutoTokenizer
from pyvi import ViTokenizer
from ...config import settings
import logging
from dagster import get_dagster_logger
import time

logger = get_dagster_logger()


def process_single_text(text: str, tokenizer, max_tokens: int, device) -> List[str]:
    """Chunk a single Vietnamese text with character and token limits."""
    character_splitter = RecursiveCharacterTextSplitter(
        separators=[". ", "!", "?", "\n\n"],
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP_CHAR,
        keep_separator=True,
    )
    text_splits = character_splitter.split_text(text)

    chunks = []
    for split in text_splits:
        inputs = tokenizer(
            split,
            add_special_tokens=False,
            return_tensors="pt",
            truncation=False
        ).to(device)

        token_count = inputs["input_ids"].shape[1]

        if token_count > max_tokens:
            token_splitter = RecursiveCharacterTextSplitter(
                chunk_size=int(max_tokens * 0.75),
                chunk_overlap=settings.CHUNK_OVERLAP_TOKEN,
                length_function=lambda x: len(tokenizer(x, add_special_tokens=False)["input_ids"]),
            )
            sub_chunks = token_splitter.split_text(split)
            chunks.extend(sub_chunks)
        else:
            chunks.append(split)

    return [chunk for chunk in chunks if len(chunk) > 10]


def chunk_text(text: str) -> List[str]:
    """Split and tokenize a single Vietnamese text into optimized chunks."""
    # Load tokenizer and device
    tokenizer = AutoTokenizer.from_pretrained(settings.EMBEDDING_MODEL_ID, trust_remote_code=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Using device: {device}")

    # Tokenize Vietnamese using Pyvi
    pyvi_start = time.time()
    tokenized_text = ViTokenizer.tokenize(text)
    logger.debug(f"Pyvi tokenization took {time.time() - pyvi_start:.2f}s")

    # Process and chunk
    chunks = process_single_text(tokenized_text, tokenizer, max_tokens=2048, device=device)

    return chunks


def chunk_multiple_texts(texts: List[str]) -> List[List[str]]:
    """Batch chunking multiple Vietnamese texts."""
    start_time = time.time()
    logger.info(f"Chunking {len(texts)} texts...")

    # Load tokenizer and device
    tokenizer = AutoTokenizer.from_pretrained(settings.EMBEDDING_MODEL_ID, trust_remote_code=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Using device: {device}")

    results = []
    for idx, text in enumerate(texts):
        pyvi_start = time.time()
        tokenized_text = ViTokenizer.tokenize(text)
        logger.debug(f"Text {idx+1}: Pyvi tokenization took {time.time() - pyvi_start:.2f}s")

        chunks = process_single_text(tokenized_text, tokenizer, max_tokens=6096, device=device)
        logger.debug(f"Text {idx+1}: {len(chunks)} chunks")
        results.append(chunks)

    logger.info(f"Batch chunking completed in {time.time() - start_time:.2f}s")
    return results
