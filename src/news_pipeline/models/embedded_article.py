from pydantic import BaseModel, Field
from bson import ObjectId
from datetime import datetime
from typing import List

class EmbeddedArticle(BaseModel):
    url: str = Field(..., description="URL of the article")
    title: str = Field(..., description="Title of the article")
    published_date: datetime = Field(..., description="Publication date")
    source_id: ObjectId = Field(..., description="Source ID")
    topic_id: ObjectId = Field(..., description="Topic ID")