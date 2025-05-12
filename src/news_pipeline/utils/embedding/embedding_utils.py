import asyncio
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from typing import List, Union
from dagster import get_dagster_logger
from ...config import settings
import os
from dotenv import load_dotenv
import time

load_dotenv()

_EMBEDDER_INSTANCE = None

def get_embedder():
    global _EMBEDDER_INSTANCE
    if _EMBEDDER_INSTANCE is None:
        _EMBEDDER_INSTANCE = Embedder()
    return _EMBEDDER_INSTANCE

class Embedder:
    def __init__(self):
        self.logger = get_dagster_logger()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.logger.info(f"Initializing Embedder on device: {self.device}")
        
        self.model = SentenceTransformer(
            settings.EMBEDDING_MODEL_ID,
            trust_remote_code=True
        ).to(self.device)
        
        # Try to use fp16 if supported
        try:
            if self.device.type == "cuda":
                self.model.half()
                self.logger.info("Using fp16 for embedding model")
        except Exception as e:
            self.logger.warning(f"Could not use fp16: {e}")

    async def encode_batch(self, batch: List[str]) -> np.ndarray:
        """Encode a batch of chunks with rate limit handling."""
        start_time = time.time()
        await asyncio.sleep(settings.SLEEP_TIME)
        
        try:
            with torch.no_grad():  # Disable gradient for efficiency
                embeddings = self.model.encode(
                    batch,
                    batch_size=len(batch),
                    show_progress_bar=False,
                    normalize_embeddings=False,
                    convert_to_numpy=True,
                    device=self.device
                )
            self.logger.debug(f"Encoded batch of {len(batch)} chunks in {time.time() - start_time:.2f}s")
            return embeddings
        except Exception as e:
            self.logger.error(f"Failed to encode batch: {e}")
            return np.array([])

    async def embed_chunks(self, chunks: List[str]) -> List[np.ndarray]:
        """Embed multiple chunks with parallel batch processing."""
        start_time = time.time()
        batches = [
            chunks[i:i + settings.BATCH_SIZE]
            for i in range(0, len(chunks), settings.BATCH_SIZE)
        ]

        semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_BATCHES)
        async def process_batch(batch: List[str]) -> np.ndarray:
            async with semaphore:
                return await self.encode_batch(batch)

        tasks = [process_batch(batch) for batch in batches]
        embeddings = await asyncio.gather(*tasks)
        
        # Flatten embeddings
        result = [emb for batch_emb in embeddings for emb in batch_emb if emb.size > 0]
        self.logger.debug(f"Embedded {len(result)} chunks in {time.time() - start_time:.2f}s")
        return result

def generate_embedding(texts: List[List[str]]) -> List[List[float]]:
    """Generate embeddings for a list of texts (each text is a list of chunks)."""
    start_time = time.time()
    logger = get_dagster_logger()

    if not texts:
        logger.warning("No texts provided for embedding")
        return []
    try:
        embedder = get_embedder()
        # Flatten all chunks
        all_chunks = [chunk for text in texts for chunk in text]
        if not all_chunks:
            logger.warning("No chunks to embed")
            return [[] for _ in texts]

        # Get embeddings
        loop = asyncio.get_event_loop()
        embeddings = loop.run_until_complete(embedder.embed_chunks(all_chunks))

        if not embeddings or not isinstance(embeddings, list):
            logger.warning("Invalid embeddings structure: not a list")
            return [[] for _ in texts]

        # Reorganize embeddings by text
        result = []
        chunk_idx = 0
        for text in texts:
            text_embeddings = []
            for _ in range(len(text)):
                if chunk_idx < len(embeddings):
                    current_emb = embeddings[chunk_idx]
                    if isinstance(current_emb, np.ndarray):
                        text_embeddings.append(current_emb.tolist())
                    elif isinstance(current_emb, list):
                        text_embeddings.append(current_emb)
                    else:
                        logger.warning(f"Skipping invalid embedding type: {type(current_emb)}")
                        text_embeddings.append([])
                    chunk_idx += 1
                else:
                    text_embeddings.append([])
            result.append(text_embeddings)

        logger.info(f"Generated {len(embeddings)} embeddings for {len(texts)} texts in {time.time() - start_time:.2f}s")
        return result
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        return [[] for _ in texts]