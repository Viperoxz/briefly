from pydantic import Field
from typing import List
from .summarized_article import SummarizedArticle

class EmbeddedArticle(SummarizedArticle):
    embeddings: List[List[float]] = Field(default_factory=list, description="List of embeddings for article chunks")