from dagster import IOManager, OutputContext, InputContext
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
import os
from dotenv import load_dotenv

load_dotenv()

class QdrantIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.client = QdrantClient(
            url=os.getenv("QDRANT_URL"),
            api_key=os.getenv("QDRANT_API_KEY"),
            prefer_grpc=True
        )
        self.collection_name = "ArticleEmbeddings"
        
        try:
            collections = self.client.get_collections()
            if self.collection_name not in [c.name for c in collections.collections]:
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=384, distance=Distance.COSINE)
                )
                print(f"Created Qdrant collection: {self.collection_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Qdrant collection: {e}")

    def store_embedding(self, point_id: str, vector: list, payload: dict):
        try:
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