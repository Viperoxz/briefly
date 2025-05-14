from dagster import asset, get_dagster_logger, Output, AssetIn, DynamicPartitionsDefinition
from ..jobs.article_jobs import article_partitions_def
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pandas as pd
import numpy as np
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import uuid
import hashlib
from bson import ObjectId
import random
import datetime

@asset(
    description="Find and store related articles for a given article using hybrid methods.",
    key="related_articles",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    kinds={"mongodb"},
    required_resource_keys={"mongo_io_manager", "qdrant_io_manager"}
)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((TimeoutError, ConnectionError))
)
def related_articles(context) -> Output[pd.DataFrame]:
    """Find and store related articles for a given article using vector similarity."""
    logger = get_dagster_logger()
    partition_key = context.partition_key
    
    try:
        # Get MongoDB and Qdrant resources
        mongo_io_manager = context.resources.mongo_io_manager
        qdrant_io_manager = context.resources.qdrant_io_manager
        collection = mongo_io_manager._get_collection(context, "articles")
        
        # Get current article
        article_doc = collection.find_one({"url": partition_key})
        if not article_doc:
            logger.warning(f"No article found for partition {partition_key}")
            return Output(value=pd.DataFrame(), metadata={"status": "article_not_found"})
            
        if article_doc.get("embedding_status") != "completed":
            logger.warning(f"Article {partition_key} does not have embeddings yet")
            return Output(value=pd.DataFrame(), metadata={"status": "no_embeddings"})
            
        # Generate point ID for Qdrant query
        url_hash = hashlib.md5(article_doc["url"].encode()).hexdigest()
        point_id = str(uuid.UUID(url_hash[:32]))
        
        try:
            point = qdrant_io_manager.client.retrieve(
                collection_name=qdrant_io_manager.collection_name,
                ids=[point_id],
                with_vectors=True
            )
            
            if not point or len(point) == 0:
                logger.warning(f"No embedding found in Qdrant for article {article_doc['url']}")
                return Output(value=pd.DataFrame(), metadata={"status": "no_embedding_in_qdrant"})
                
            query_vector = point[0].vector
            
            # Query Qdrant for similar articles
            search_result = qdrant_io_manager.client.query_points(
                collection_name=qdrant_io_manager.collection_name,
                query=query_vector,
                query_filter=Filter(
                    must=[
                        # Find articles with the same topic name
                        FieldCondition(
                            key="topic_id",
                            match=MatchValue(value=str(article_doc.get("topic_id")))
                        )
                    ],
                    must_not=[
                        # Exclude the current article
                        FieldCondition(
                            key="url",
                            match=MatchValue(value=article_doc["url"])
                        )
                    ]
                ),
                search_params={"hnsw_ef": 128},
                with_payload=True,
                with_vectors=False,
                limit=6
            )

            if len(search_result.points) < 6:
                logger.info(f"Found only {len(search_result.points)} articles with the same topic_id. Searching without topic filter.")
                remain_count = 6 - len(search_result.points)
                existing_urls = [hit.payload.get("url") for hit in search_result.points]
                existing_urls.append(article_doc["url"])
                
                additional_results = qdrant_io_manager.client.query_points(
                    collection_name=qdrant_io_manager.collection_name,
                    query=query_vector,
                    query_filter=Filter(
                        must_not=[
                            FieldCondition(
                                key="url", 
                                match=MatchValue(value=url)
                            ) for url in existing_urls
                        ]
                    ),
                    search_params={"hnsw_ef": 128},
                    with_payload=True,
                    with_vectors=False,
                    limit=remain_count
                )
                search_result.points.extend(additional_results.points)
                logger.info(f"Found additional {len(additional_results.points)} articles without topic_id filter.")
        except Exception as e:
            logger.error(f"Error retrieving embedding from Qdrant: {str(e)}")
            return Output(value=pd.DataFrame(), metadata={"status": "qdrant_retrieval_error", "error": str(e)})
        
        combined_results = []
        for hit in search_result.points:
            hit_url = hit.payload.get("url")
            # Fetch the article from MongoDB
            related_article = collection.find_one({"url": hit_url}, {"_id": 1, "validation_score": 1})
            if related_article:
                hit_id = related_article.get("_id")
                validation_score = related_article.get("validation_score", random.randint(3, 6))
            else:
                continue

            similarity_weight = 0.7
            validation_height = 0.3
            combined_score = (similarity_weight * hit.score * 10) + (validation_height * validation_score)  

            combined_results.append({
                "id": hit_id,
                "combined_score": combined_score
            })
        
        combined_results.sort(key=lambda x: x["combined_score"], reverse=True)
        final_results = combined_results[:5]  
        # Update MongoDB with related articles
        collection.update_one(
            {"url": article_doc["url"]},
            {"$set": {
                "related_ids": [result["id"] for result in final_results], 
                "related_ids_updated_at": datetime.datetime.now(datetime.timezone.utc)
            }}
        )

        logger.info(f"✅ Successfully found {len(search_result.points)} related articles for {article_doc['url']}")
        return Output(
            value=pd.DataFrame([{"url": article_doc["url"], "num_related": len(search_result.points)}]),
            metadata={"num_related": len(search_result.points), "status": "success"}
        )

    except Exception as e:
        logger.error(f"Error finding related articles for {partition_key}: {str(e)}")
        return Output(value=pd.DataFrame(), metadata={"status": "error", "error": str(e)})