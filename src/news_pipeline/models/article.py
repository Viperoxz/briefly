from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional
from datetime import datetime
import re
import os
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()

class Article(BaseModel):
    id: ObjectId = Field(alias="_id", description="MongoDB ObjectId of the article")
    source_id: ObjectId = Field(..., description="ID of the article source")
    topic_id: ObjectId = Field(..., description="ID of the article topic")
    title: str = Field(..., description="Title of the article")
    url: str = Field(..., description="URL of the article")
    image: str = Field(..., description="URL of the article's image")
    published_date: datetime = Field(..., description="Publication date of the article")
    content: str = Field(..., description="Full content of the article")
    alias: str = Field(..., description="Alias of the article title")
    summary: Optional[list[str]] = Field(None, description="Summary of the article")
    audio_url: Optional[ObjectId] = Field(None, description="URL of the audio version of the article")

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  
        extra="ignore",  
        json_encoders={  
            ObjectId: lambda v: str(v)
        }
    )

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, v):
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        if not db["sources"].find_one({"_id": v}):
            raise ValueError(f"Source ID '{v}' not found in sources collection")
        return v

    @field_validator("topic_id")
    @classmethod
    def validate_topic_id(cls, v):
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB")]
        if not db["topics"].find_one({"_id": v}):
            raise ValueError(f"Topic ID '{v}' not found in topics collection")
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, v):
            raise ValueError(f"Invalid URL for url: {v}")
        return v

    @field_validator("image")
    @classmethod
    def validate_image(cls, v):
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, v):
            raise ValueError(f"Invalid URL for image: {v}")
        return v

    @field_validator("content")
    @classmethod
    def validate_content(cls, v):
        if len(v) < 20:
            raise ValueError("Content is too short (minimum 20 characters)")
        return v

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, v):
        if not v or len(v.strip()) < 1:
            raise ValueError("Alias cannot be empty")
        return v