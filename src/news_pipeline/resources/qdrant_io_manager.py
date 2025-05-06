from dagster import IOManager, OutputContext, InputContext
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from typing import List, Dict, Any
from ..config import settings
import os
from dotenv import load_dotenv

load_dotenv()

class QdrantIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.client = QdrantClient(
            url=os.getenv("QDRANT_URL"),
            api_key=os.getenv("QDRANT_API_KEY")
        )
        self.collection_name = "ArticleEmbeddings"
        
        # Initialize Qdrant collection
        self.initialize_qdrant()

    def initialize_qdrant(self):
        """Initialize Qdrant collection if it doesn't exist."""
        try:
            collections = self.client.get_collections()
            if self.collection_name not in [c.name for c in collections.collections]:
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=settings.EMBEDDING_VECTOR_SIZE,
                        distance=Distance.COSINE
                    )
                )
                # Create payload indexes for efficient filtering
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="source",
                    field_schema="keyword"
                )
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="topic",
                    field_schema="keyword"
                )
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="published",
                    field_schema="datetime"
                )
                print(f"Created Qdrant collection: {self.collection_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Qdrant collection: {e}")

    def store_embeddings(self, points: List[PointStruct]):
        """Store multiple embeddings in Qdrant using batch upsert."""
        try:
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
        except Exception as e:
            raise RuntimeError(f"Failed to store embeddings in Qdrant: {e}")

    def handle_output(self, context: OutputContext, obj: List[Dict[str, Any]]):
        """Handle output for embedded articles."""
        points = []
        for article in obj:
            link = article["link"]
            for idx, embedding in enumerate(article.get("embeddings", [])):
                if embedding:  # Only store valid embeddings
                    point_id = f"{link}_chunk_{idx}"
                    points.append(
                        PointStruct(
                            id=point_id,
                            vector=embedding,
                            payload={
                                "article_link": link,
                                "source": article["source"],
                                "topic": article["topic"],
                                "chunk_index": idx,
                                "title": article["title"],
                                "summary": article.get("summary", ""),
                                "published": article["published"]
                            }
                        )
                    )
        
        if points:
            self.store_embeddings(points)
            context.log.info(f"Stored {len(points)} points in Qdrant collection '{self.collection_name}'")
        else:
            context.log.warning("No valid embeddings to store in Qdrant")

    def load_input(self, context: InputContext):
        """Load embeddings from Qdrant (for recommendation)."""
        try:
            points = self.client.scroll(
                collection_name=self.collection_name,
                limit=100,
                with_vectors=True
            )[0]
            return points
        except Exception as e:
            raise RuntimeError(f"Failed to load embeddings from Qdrant: {e}")