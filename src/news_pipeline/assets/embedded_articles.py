from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from ..models import Article
import pandas as pd
import numpy as np
import uuid
import hashlib
import gc
import torch

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="embedded_articles",
    io_manager_key="mongo_io_manager",  
    partitions_def=article_partitions_def,
    kinds={"huggingface", "pytorch"}
)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((TimeoutError, ConnectionError))
)
def embedded_articles(context) -> Output[pd.DataFrame]:
    logger = get_dagster_logger()
    partition_key = context.partition_key

    try:
        # Lấy document từ MongoDB
        mongo_io_manager = context.resources.mongo_io_manager
        collection = mongo_io_manager._get_collection(context, "articles")
        article_doc = collection.find_one({"url": partition_key})

        if not article_doc:
            logger.warning(f"No article found for partition {partition_key}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "article_not_found"})
        if not article_doc.get("summary"):
            logger.warning(f"Article {partition_key} does not have a summary yet")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_summary"})

        # Khởi tạo model và xử lý text
        article_model = Article(**article_doc)
        cleaned_content = clean_text(article_model.summary)
        logger.info(f"Cleaned content length: {len(cleaned_content)}")

        chunks = chunk_text(cleaned_content)
        logger.info(f"Number of chunks generated: {len(chunks)}")
        if not chunks:
            logger.warning(f"No chunks generated for {article_model.url}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_chunks"})

        # Generate embeddings với timeout handling
        embeddings_result = None
        try:
            from contextlib import timeout
            with timeout(60):
                embeddings_result = generate_embedding([chunks])
            
            # Kiểm tra kết quả ngay sau khi tạo
            if not embeddings_result or len(embeddings_result) == 0:
                logger.warning(f"No embeddings generated for {article_model.url}")
                return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "no_embeddings"})
                
        except TimeoutError:
            logger.error(f"Embedding generation took too long for {article_model.url} and was aborted")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "timeout"})
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "embedding_error"})

        logger.info(f"Generated embeddings for {len(embeddings_result)} chunks")
        
        # Xử lý embeddings
        embeddings_list = embeddings_result[0]
        logger.info(f"Embeddings list length: {len(embeddings_list)}")
        
        if not embeddings_list or not any(embeddings_list):
            logger.warning(f"Empty embeddings for {article_model.url}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "empty_embeddings"})
        
        # Tính trung bình các embeddings
        try:
            if all(isinstance(emb, list) and len(emb) > 0 for emb in embeddings_list):
                embeddings = np.mean([np.array(emb) for emb in embeddings_list], axis=0).tolist()
            else:
                valid_embeddings = [emb for emb in embeddings_list if isinstance(emb, list) and len(emb) > 0]
                if valid_embeddings:
                    embeddings = np.mean([np.array(emb) for emb in valid_embeddings], axis=0).tolist()
                else:
                    logger.error(f"Invalid embedding structure for {article_model.url}")
                    return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "invalid_embedding"})
        except Exception as e:
            logger.error(f"Error averaging embeddings: {e}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "status": "averaging_error"})
        
        # Lấy thông tin source và topic
        source_doc = mongo_io_manager._get_collection(context, "sources").find_one({"_id": article_model.source_id})
        topic_doc = mongo_io_manager._get_collection(context, "topics").find_one({"_id": article_model.topic_id})
        source_name = source_doc["name"] if source_doc else ""
        topic_name = topic_doc["name"] if topic_doc else ""

        # Lưu embedding vào Qdrant
        payload = {
            "title": article_model.title,
            "source_name": source_name,
            "topic_name": topic_name, 
            "published_date": article_model.published_date.isoformat(),
            "url": article_model.url  # Thêm URL vào payload để dễ truy xuất
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
        embedded_article.has_embedding = True
        
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