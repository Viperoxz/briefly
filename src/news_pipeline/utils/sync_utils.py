from dagster import get_dagster_logger
from pymongo import MongoClient
from qdrant_client import QdrantClient

def sync_mongo_qdrant(mongo_uri: str, mongo_db: str, qdrant_url: str, qdrant_api_key: str):
    logger = get_dagster_logger()
    mongo_client = MongoClient(mongo_uri)
    qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
    
    db = mongo_client[mongo_db]
    collection = db["Articles"]
    
    # Get all MongoDB article links
    mongo_articles = collection.find({}, {"link": 1})
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
    
    # Log missing MongoDB articles (for debugging)
    missing_in_mongo = mongo_links - qdrant_ids
    if missing_in_mongo:
        logger.warning(f"Found {len(missing_in_mongo)} articles in MongoDB without Qdrant embeddings")