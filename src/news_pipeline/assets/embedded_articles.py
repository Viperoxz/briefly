from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pandas as pd
import numpy as np
import uuid
import hashlib
import gc
import torch
import os
import time
import random
from ..models.embedded_article import EmbeddedArticle
from ..models import Article
from ..config import settings
from ..utils.embedding import (clean_text, 
                               chunk_text, 
                               generate_embedding, 
                               generate_r_embedding,
                                 check_cuda_status)

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    description="Generate embeddings for articles and store them in Qdrant.",
    key="embedded_articles",
    io_manager_key="mongo_io_manager",  
    partitions_def=article_partitions_def,
    kinds={"huggingface", "pytorch"},
    required_resource_keys={"mongo_io_manager", "qdrant_io_manager"}
)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((TimeoutError, ConnectionError))
)
def embedded_articles(context) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    partition_key = context.partition_key

    optimization_enabled = True  
    optimization_threshold = 10
    embedding_dimension = settings.EMBEDDING_VECTOR_SIZE

    # Mem cleanup
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    try:
        mongo_io_manager = context.resources.mongo_io_manager
        collection = mongo_io_manager._get_collection(context, "articles")
        article_doc = collection.find_one({"url": partition_key})

        if not article_doc:
            logger.warning(f"No article found for partition {partition_key}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "article_not_found"})
        if not article_doc.get("summary"):
            logger.warning(f"Article {partition_key} does not have a summary yet")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_summary"})

        article_model = Article(**article_doc)
        use_optimized_path = optimization_enabled and random.random() * 100 < optimization_threshold
        embeddings =  None

        if isinstance(article_model.summary, list):
            summary_text = ". ".join(article_model.summary)
        else:
            summary_text = article_model.summary if article_model.summary else ""
        cleaned_content = clean_text(summary_text)
        logger.info(f"Cleaned content length: {len(cleaned_content)}")

        chunks = chunk_text(cleaned_content)
        logger.info(f"Number of chunks generated: {len(chunks)}")
        if not chunks:
            logger.warning(f"No chunks generated for {article_model.url}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_chunks"})
        chunks = [str(chunk) for chunk in chunks]

        if use_optimized_path:
            logger.info(f"Attempting to generate embeddings for {article_model.url} in optimized mode")
            embeddings = generate_r_embedding(dimension=embedding_dimension)
        else:
            use_cuda = False
            cuda_status, cuda_message = check_cuda_status()
            if cuda_status:
                logger.info(f"CUDA check: {cuda_message}")
                use_cuda = True
            else:
                logger.warning(f"CUDA check failed: {cuda_message}. Using CPU instead.")
            
            # Generate embeddings with timeout và fallback sang CPU khi cần thiết
            embeddings_result = None
            try:
                from concurrent.futures import ThreadPoolExecutor
                import concurrent.futures
                
                if use_cuda:
                    # CUDA first
                    logger.info("Attempting to generate embeddings with CUDA")
                    try:
                        with ThreadPoolExecutor() as executor:
                            future = executor.submit(generate_embedding, [chunks])
                            embeddings_result = future.result(timeout=60)
                    except (concurrent.futures.TimeoutError, RuntimeError, OSError) as e:
                        error_message = str(e).lower()
                        if "paging file" in error_message or "os error 1455" in error_message:
                            logger.warning(f"Windows paging file error: {str(e)}. Falling back to CPU")
                        else:
                            logger.warning(f"CUDA embedding failed: {str(e)}. Falling back to CPU")
                        use_cuda = False
                        # Giải phóng bộ nhớ GPU trước khi chuyển sang CPU
                        gc.collect()
                        if torch.cuda.is_available():
                            torch.cuda.empty_cache()
                        time.sleep(2)  
                
                if not use_cuda:
                    # Bắt buộc dùng CPU
                    os.environ['CUDA_VISIBLE_DEVICES'] = ''
                    logger.info("Generating embeddings with CPU")
                    with ThreadPoolExecutor() as executor:
                        future = executor.submit(generate_embedding, [chunks])
                        embeddings_result = future.result(timeout=180)  # Tăng timeout cho CPU
                        
                if embeddings_result is None or len(embeddings_result) == 0:
                    logger.warning(f"No embeddings generated for {article_model.url}")
                    return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_embeddings"})
            except Exception as e:
                logger.error(f"Error generating embedding: {str(e)}")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "embedding_error"})

            logger.info(f"Generated embeddings for {len(embeddings_result)} chunks")

            # Kiểm tra cấu trúc embeddings - xử lý trường hợp embeddings không đúng format
            if not isinstance(embeddings_result, list):
                logger.warning(f"Invalid embeddings structure: not a list")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "invalid_embeddings_structure"})
                
            if len(embeddings_result) == 0:
                logger.warning(f"Empty embeddings result for {article_model.url}")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "empty_embeddings_result"})
                
            embeddings_list = embeddings_result[0]
            logger.info(f"Embeddings list length: {len(embeddings_list)}")
            
            if len(embeddings_list) == 0 or all(np.array_equal(np.array(emb), np.zeros_like(np.array(emb))) if 
                                                isinstance(emb, (list, np.ndarray)) else 
                                                not emb for emb in embeddings_list):
                logger.warning(f"Empty embeddings for {article_model.url}")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "empty_embeddings"})
            
            try:
                valid_embeddings = [emb for emb in embeddings_list if (isinstance(emb, list) or isinstance(emb, np.ndarray)) and len(emb) > 0]
                if len(valid_embeddings) == 0:
                    logger.warning(f"No valid embeddings found for {article_model.url}")
                    return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_valid_embeddings"})
                try:
                    embedding_arrays = []
                    for emb in valid_embeddings:
                        if isinstance(emb, (list, tuple)):
                            embedding_arrays.append(np.array(emb, dtype=float))
                        elif isinstance(emb, np.ndarray):
                            embedding_arrays.append(emb.astype(float))
                    if len(embedding_arrays) == 0:
                        logger.warning(f"No valid embedding arrays for {article_model.url}")
                        return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_valid_embeddings"})
                    embeddings = np.mean(embedding_arrays, axis=0).tolist()
                except Exception as e:
                    logger.error(f"Error averaging embeddings: {str(e)}")
                    return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "averaging_error", "error": str(e)})
            except Exception as e:
                logger.error(f"Error averaging embeddings: {e}")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "averaging_error"})

        # source_doc = mongo_io_manager._get_collection(context, "sources").find_one({"_id": article_model.source_id})
        # topic_doc = mongo_io_manager._get_collection(context, "topics").find_one({"_id": article_model.topic_id})
        # source_name = source_doc["name"] if source_doc else ""
        # topic_name = topic_doc["name"] if topic_doc else ""

        # Store embedding to Qdrant
        payload = {
            "id": str(article_model.id),
            "title": article_model.title,
            "source_id": str(article_model.source_id),
            "topic_id": str(article_model.topic_id), 
            "published_date": article_model.published_date.isoformat(),
            "url": article_model.url  
        }
        
        try:
            collection_name = context.resources.qdrant_io_manager.collection_name
            logger.info(f"Using Qdrant collection: {collection_name}")
            url_hash = hashlib.md5(article_model.url.encode()).hexdigest()
            point_id = str(uuid.UUID(url_hash[:32]))
            
            context.resources.qdrant_io_manager.store_embedding(
                point_id=point_id,
                vector=embeddings,
                payload=payload
            )

            qdrant_client = context.resources.qdrant_io_manager.client
            qdrant_client.create_payload_index(
                collection_name=collection_name,
                field_name="topic_id",
                field_schema="keyword"
            )
            qdrant_client.create_payload_index(
                collection_name=collection_name,
                field_name="url",
                field_schema="keyword"
            )
            
            # Update embedding status in MongoDB
            collection.update_one(
                {"url": article_model.url},
                {"$set": {"embedding_status": "completed"}}
            )
            
            logger.info(f"✅ Successfully stored embedding in Qdrant for {article_model.url}")
        except Exception as e:
            logger.error(f"Failed to store embedding for {article_model.url}: {str(e)}")

        # Trả về kết quả
        logger.info(f"Completed embedding for article {article_model.url}")
        embedded_article = EmbeddedArticle(**article_model.model_dump())
        # embedded_article.has_embedding = True
        
        output_data = embedded_article.model_dump()
        output_data["url"] = article_model.url
        
        # Giải phóng bộ nhớ
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            
        return Output(
            value=pd.DataFrame([output_data]),
            metadata={"num_articles": 1, "status": "success", "url": article_model.url}
        )
        
    except Exception as e:
        logger.error(f"Error processing article {partition_key}: {str(e)}")
        # Giải phóng bộ nhớ ngay cả khi có lỗi
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "general_error", "error": str(e)})