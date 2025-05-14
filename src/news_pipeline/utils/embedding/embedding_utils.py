import asyncio
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from typing import List, Union
from dagster import get_dagster_logger
import gc
import math
import random
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

        use_cpu = os.getenv("USE_CPU_FOR_EMBEDDING", "false").lower() == "true"
        if use_cpu:
            self.device = torch.device("cpu")
            self.logger.info("Forced CPU usage for embedding based on environment settings")
        else:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
        self.logger.info(f"Initializing Embedder on device: {self.device}")

        self.model = SentenceTransformer(
            settings.EMBEDDING_MODEL_ID,
            trust_remote_code=True,
            device=self.device,
        )
        
        try:
            if self.device.type == "cuda" and os.getenv("EMBEDDING_PRECISION", "fp16") == "fp16":
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


async def _embed_chunks_async(embedder, chunks):
    return await embedder.embed_chunks(chunks)

def generate_r_embedding(dimension: int = 768) -> List[float]:
    """Generate a random embedding vector of specified dimension."""
    return list(np.random.normal(0, 0.1, dimension))

def generate_embedding(texts: List[List[str]]) -> List[List[float]]:
    """Generate embeddings for a list of texts (each text is a list of chunks)."""
    start_time = time.time()
    logger = get_dagster_logger()

    if not texts:
        logger.warning("No texts provided for embedding")
        return []
        
    try:
        # Kiểm tra và ghi log cấu hình bộ nhớ hiện tại
        if torch.cuda.is_available():
            logger.info(f"Using device: {torch.cuda.get_device_name(0)}")
            logger.info(f"Total GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
            logger.info(f"Available GPU memory: {torch.cuda.memory_reserved(0) / 1e9:.2f} GB")
        
        # Flatten all chunks 
        all_chunks = [chunk for text in texts for chunk in text if chunk.strip()]
        if not all_chunks:
            logger.warning("No valid chunks to embed")
            return [[] for _ in texts]

        batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", "2"))
        logger.info(f"Using batch size: {batch_size}")
        
        embedder = get_embedder()
        
        # Xử lý từng batch để giảm thiểu sử dụng bộ nhớ
        all_embeddings = []
        num_batches = math.ceil(len(all_chunks) / batch_size)
        
        for i in range(0, len(all_chunks), batch_size):
            batch = all_chunks[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{num_batches} with {len(batch)} chunks")
            
            try:
                batch_embeddings = asyncio.run(embedder.embed_chunks(batch))
                all_embeddings.extend(batch_embeddings)
                logger.info(f"Batch {i//batch_size + 1} completed successfully")
            except Exception as e:
                logger.error(f"Error embedding batch {i//batch_size + 1}: {str(e)}")
                # Nếu xử lý batch lỗi, thử với từng chunk một
                for j, chunk in enumerate(batch):
                    try:
                        single_embedding = asyncio.run(embedder.embed_chunks([chunk]))
                        all_embeddings.extend(single_embedding)
                        logger.info(f"Processed chunk {j+1}/{len(batch)} individually")
                    except Exception as chunk_e:
                        logger.error(f"Error embedding individual chunk {j+1}: {str(chunk_e)}")
                        # Thêm embedding rỗng nếu không thể tạo được
                        all_embeddings.append([0.0] * 768)  # Thông thường embedding size là 768
            
            # Force giải phóng bộ nhớ sau mỗi batch
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            time.sleep(1)

        result = []
        offset = 0
        for text in texts:
            if not text or not any(t.strip() for t in text):
                result.append([])
                continue

            valid_chunks = sum(1 for t in text if t.strip())
            if valid_chunks == 0:
                result.append([])
                continue

            text_embeddings = all_embeddings[offset:offset+valid_chunks]
            offset += valid_chunks
            result.append(text_embeddings)
        
        elapsed = time.time() - start_time
        logger.info(f"Embedding generation completed in {elapsed:.2f}s")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {str(e)}")
        return [[] for _ in texts]
    finally:
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

# Check CUDA status
def check_cuda_status():
    if not torch.cuda.is_available():
        return False, "CUDA not available"
    
    try:
        x = torch.tensor([1.0, 2.0], device="cuda")
        y = x * 2
        del x, y
        torch.cuda.empty_cache()
        return True, "CUDA working properly"
    except Exception as e:
        return False, f"CUDA error: {str(e)}"