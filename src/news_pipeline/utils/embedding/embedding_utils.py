import asyncio
import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Union
from dagster import get_dagster_logger
from ...config import settings
import os
from dotenv import load_dotenv

load_dotenv()

class Embedder:
    def __init__(self):
        self.logger = get_dagster_logger()
        self.model = SentenceTransformer(
            settings.EMBEDDING_MODEL_ID,
            trust_remote_code=True  
        )
        # Try to use fp16 if supported
        try:
            self.model.half()
            self.logger.info("Using fp16 for embedding model")
        except Exception as e:
            self.logger.warning(f"Could not use fp16: {e}")

    async def encode_batch(self, batch: List[str]) -> np.ndarray:
        """Encode a batch of chunks with rate limit handling."""
        await asyncio.sleep(settings.SLEEP_TIME)
        try:
            embeddings = self.model.encode(
                batch,
                batch_size=len(batch),
                show_progress_bar=False,
                normalize_embeddings=False,
                convert_to_numpy=True,
            )
            self.logger.info(f"Encoded batch of {len(batch)} chunks")
            return embeddings
        except Exception as e:
            self.logger.error(f"Failed to encode batch: {e}")
            return np.array([])

    async def embed_chunks(self, chunks: List[str]) -> List[np.ndarray]:
        """Embed multiple chunks with parallel batch processing."""
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
        return [emb for batch_emb in embeddings for emb in batch_emb if emb.size > 0]

def generate_embedding(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a list of texts (each text is a list of chunks)."""
    logger = get_dagster_logger()
    try:
        embedder = Embedder()
        # Flatten all chunks
        all_chunks = [chunk for text in texts for chunk in text]
        if not all_chunks:
            logger.warning("No chunks to embed")
            return [[] for _ in texts]

        # Get embeddings
        loop = asyncio.get_event_loop()
        embeddings = loop.run_until_complete(embedder.embed_chunks(all_chunks))

        # Reorganize embeddings by text
        result = []
        chunk_idx = 0
        for text in texts:
            text_embeddings = []
            for _ in range(len(text)):
                if chunk_idx < len(embeddings):
                    text_embeddings.append(embeddings[chunk_idx].tolist())
                    chunk_idx += 1
                else:
                    text_embeddings.append([])
            result.append(text_embeddings)

        logger.info(f"Generated {len(embeddings)} embeddings for {len(texts)} texts")
        return result
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        return [[] for _ in texts]