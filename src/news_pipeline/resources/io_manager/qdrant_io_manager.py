from dagster import IOManager, OutputContext, InputContext
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import os
from dotenv import load_dotenv
from ...config import settings

load_dotenv()

class QdrantIOManager(IOManager):
    def __init__(self, config):
        self._config = config

        client_params = {
            "url": config.get("url"),
            "prefer_grpc": False,
            "timeout": 90.0,
        }
        if "api_key" in config and config["api_key"]:
            client_params["api_key"] = config["api_key"]
        
        self.client = QdrantClient(**client_params)
        self.collection_name = os.getenv("QDRANT_COLLECTION_NAME", "articles-embeddings")

        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                collections = self.client.get_collections()
                if self.collection_name not in [c.name for c in collections.collections]:
                    self.client.create_collection(
                        collection_name=self.collection_name,
                        vectors_config=VectorParams(size=settings.EMBEDDING_VECTOR_SIZE, distance=Distance.COSINE)
                    )
                    print(f"Created Qdrant collection: {self.collection_name}")
                break
            except Exception as e:
              retry_count += 1
              if retry_count >= max_retries:
                    raise RuntimeError(f"Failed to initialize Qdrant collection after {max_retries} attempts: {e}")
              import time
              time.sleep(2)

    def store_embedding(self, point_id: str, vector: list, payload: dict):
        try:
            import uuid
            import hashlib

            try:
                uuid.UUID(point_id)
            except ValueError:
                hash_obj = hashlib.md5(point_id.encode()).hexdigest()
                point_id = str(uuid.UUID(hash_obj[:32]))  
                
            point = PointStruct(
                id=point_id,
                vector=vector,
                payload=payload
            )
            self.client.upsert(
                collection_name=self.collection_name,
                points=[point]
            )
        except Exception as e:
            raise RuntimeError(f"Failed to store embedding in Qdrant: {e}")

    def handle_output(self, context: OutputContext, obj):
        pass  # Handled by store_embedding

    def load_input(self, context: InputContext):
        try:
            points = self.client.scroll(
                collection_name=self.collection_name,
                limit=100,
                with_vectors=True
            )[0]
            return points
        except Exception as e:
            raise RuntimeError(f"Failed to load embeddings from Qdrant: {e}")