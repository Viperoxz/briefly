from dagster import get_dagster_logger
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
from .embedding import clean_text, chunk_text, generate_embedding
import numpy as np

def sync_mongo_qdrant(mongo_uri: str, mongo_db: str, qdrant_url: str, qdrant_api_key: str):
    logger = get_dagster_logger()
    mongo_client = MongoClient(mongo_uri)
    qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
    
    db = mongo_client[mongo_db]
    collection = db["articles"]  # Sửa Articles thành articles để khớp với mongo_io_manager
    
    # Get all MongoDB article URLs and necessary fields
    mongo_articles = collection.find({}, {"url": 1, "content": 1, "source_id": 1, "topic_id": 1, "title": 1})
    mongo_urls = set(article["url"] for article in mongo_articles)
    
    # Get all Qdrant point IDs
    points = qdrant_client.scroll(collection_name="ArticleEmbeddings", limit=100)[0]
    qdrant_ids = set(point.id for point in points)
    
    # Delete orphaned Qdrant embeddings
    orphaned_ids = qdrant_ids - mongo_urls
    if orphaned_ids:
        qdrant_client.delete(
            collection_name="ArticleEmbeddings",
            points_selector=list(orphaned_ids)
        )
        logger.info(f"Deleted {len(orphaned_ids)} orphaned Qdrant embeddings")
    
    # Create missing embeddings for MongoDB articles
    missing_in_qdrant = mongo_urls - qdrant_ids
    for url in missing_in_qdrant:
        article = collection.find_one({"url": url})
        try:
            cleaned_content = clean_text(article["content"])
            chunks = chunk_text(cleaned_content)
            if not chunks:
                logger.warning(f"No chunks generated for article {url}")
                continue
            embeddings_list = generate_embedding([chunks])[0]
            if not embeddings_list:
                logger.warning(f"No embeddings generated for article {url}")
                continue
            
            # Average embeddings across chunks
            embeddings = np.mean(embeddings_list, axis=0).tolist()
            
            # Lấy source và topic name từ collections sources và topics
            source_doc = db["sources"].find_one({"_id": article["source_id"]})
            topic_doc = db["topics"].find_one({"_id": article["topic_id"]})
            source_name = source_doc["name"] if source_doc else ""
            topic_name = topic_doc["name"] if topic_doc else ""
            
            qdrant_client.upsert(
                collection_name="ArticleEmbeddings",
                points=[PointStruct(
                    id=url,
                    vector=embeddings,
                    payload={
                        "source_id": str(article["source_id"]),
                        "topic_id": str(article["topic_id"]),
                        "source_name": source_name,
                        "topic_name": topic_name,
                        "title": article.get("title", "")
                    }
                )]
            )
            logger.info(f"Created embedding for {url} in Qdrant")
        except Exception as e:
            logger.error(f"Failed to create embedding for {url}: {e}")
            continue