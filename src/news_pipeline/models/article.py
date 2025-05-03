from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime
from pathlib import Path
import re

base_dir = Path(__file__).resolve().parent.parent.parent.parent
SOURCES = set()
with open(base_dir / "config" / "sources", "r", encoding="utf-8") as f:
    SOURCES.update(line.strip() for line in f if line.strip())
TOPICS = set()
with open(base_dir / "config" / "topics", "r", encoding="utf-8") as f:
    TOPICS.update(line.strip() for line in f if line.strip())

class Article(BaseModel):
    source: str = Field(..., description="Source of the article")
    topic: str = Field(..., description="Topic of the article") 
    title: str = Field(..., description="Title of the article")
    link: str = Field(..., description="URL of the article")
    image: Optional[str] = Field(None, description="URL of the article's image")
    published: str = Field(..., description="Publication date of the article")
    content: str = Field(..., description="Full content of the article")

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        if v not in SOURCES:
            raise ValueError(f"Source '{v}' not in allowed sources: {SOURCES}")
        return v

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v):
        if v not in TOPICS:
            raise ValueError(f"Topic '{v}' not in allowed topics: {TOPICS}")
        return v

    @field_validator("link")
    @classmethod
    def validate_link(cls, v):
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, v):
            raise ValueError(f"Invalid URL for link: {v}")
        return v

    @field_validator("image", mode='before')
    @classmethod
    def validate_image(cls, v):
        if v is not None:
            url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            if not re.match(url_pattern, v):
                raise ValueError(f"Invalid URL for image: {v}")
        return v

    @field_validator("content")
    @classmethod
    def validate_content(cls, v):
        if len(v) < 50:
            raise ValueError("Content is too short (minimum 50 characters)")
        return v

    class Config:
        extra = "ignore"
