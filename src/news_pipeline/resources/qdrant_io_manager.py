from dagster import IOManager, OutputContext
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct
import os
from ..config import settings

class QdrantIOManager(IOManager):
    def __init__(self, config: Dict[str, str]):
        self.client = QdrantClient(
            url=config["QDRANT_URL"],
            api_key=config["QDRANT_API_KEY"]
        )
        self.collection_name = config["QDRANT_COLLECTION"]
        # Initialize collection if not exists
        try:
            self.client.get_collection(self.collection_name)
        except Exception:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={"size": 768, "distance": "Cosine"}
            )

    def store_embeddings(self, points: List[PointStruct]):
        """Store embeddings into Qdrant."""
        self.client.upsert(
            collection_name=self.collection_name,
            points=points
        )

    def handle_output(self, context: OutputContext, obj: Dict):
        """Handle output from embedded_articles and store in Qdrant."""
        if not obj:  # Skip empty objects (from errors in embedded_articles)
            return

        points = []
        link = obj["link"]
        for idx, embedding in enumerate(obj.get("embeddings", [])):
            if embedding:
                point_id = f"{link}_chunk_{idx}"
                points.append(
                    PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload={
                            "article_link": link,
                            "source": obj["source"],
                            "topic": obj["topic"],
                            "chunk_index": idx,
                            "title": obj["title"],
                            "summary": obj["summary"],
                            "published": obj["published"]
                        }
                    )
                )

        if points:
            self.store_embeddings(points)

    def load_input(self, context):
        raise NotImplementedError("QdrantIOManager does not support loading inputs")