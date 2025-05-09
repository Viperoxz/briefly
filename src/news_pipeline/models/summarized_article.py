from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime
from bson import ObjectId

class SummarizedArticle(BaseModel):
    url: str = Field(..., description="URL of the article, used as unique identifier")
    title: str = Field(..., description="Title of the article")
    published_date: datetime = Field(..., description="Publication date of the article")
    source_id: ObjectId = Field(..., description="ID of the source in MongoDB")
    topic_id: ObjectId = Field(..., description="ID of the topic in MongoDB")
    summary: str = Field(..., description="Summarized content of the article")
    summary_status: Optional[str] = Field(None, description="Status of summary validation (passed/failed)")

    # @field_validator("summary")
    # @classmethod
    # def validate_summary(cls, v):
    #     if len(v) < 15:
    #         raise ValueError("Summary is too short (minimum 15 characters)")
    #     return v
