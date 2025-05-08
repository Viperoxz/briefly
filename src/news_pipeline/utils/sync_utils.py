from dagster import get_dagster_logger
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
from .embedding import clean_text, chunk_text, generate_embedding

def sync_mongo_qdrant(mongo_uri: str, mongo_db: str, qdrant_url: str, qdrant_api_key: str):
    logger = get_dagster_logger()
    mongo_client = MongoClient(mongo_uri)
    qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
    
    db = mongo_client[mongo_db]
    collection = db["Articles"]
    
    # Get all MongoDB article links and content
    mongo_articles = collection.find({}, {"link": 1, "content": 1, "source": 1, "topic": 1, "title": 1})
    mongo_links = set(article["link"] for article in mongo_articles)
    
    # Get all Qdrant point IDs
    points = qdrant_client.scroll(collection_name="ArticleEmbeddings", limit=100)[0]
    qdrant_ids = set(point.id for point in points)
    
    # Delete orphaned Qdrant embeddings
    orphaned_ids = qdrant_ids - mongo_links
    if orphaned_ids:
        qdrant_client.delete(
            collection_name="ArticleEmbeddings",
            points_selector=list(orphaned_ids)
        )
        logger.info(f"Deleted {len(orphaned_ids)} orphaned Qdrant embeddings")
    
    # Create missing embeddings for MongoDB articles
    missing_in_qdrant = mongo_links - qdrant_ids
    for link in missing_in_qdrant:
        article = collection.find_one({"link": link})
        try:
            cleaned_content = clean_text(article["content"])
            chunks = chunk_text(cleaned_content)
            embeddings = generate_embedding([chunks])[0]
            qdrant_client.upsert(
                collection_name="ArticleEmbeddings",
                points=[PointStruct(
                    id=link,
                    vector=embeddings[0],
                    payload={
                        "source": article.get("source", ""),
                        "topic": article.get("topic", ""),
                        "title": article.get("title", "")
                    }
                )]
            )
            logger.info(f"Created embedding for {link} in Qdrant")
        except Exception as e:
            logger.error(f"Failed to create embedding for {link}: {e}")