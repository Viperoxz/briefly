from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional
from datetime import datetime
import re
import os
import hashlib
from uuid import uuid4
import langid
from dotenv import load_dotenv

load_dotenv()

class ProcessedArticle(BaseModel):
    article_id: str = Field(
        ...,
        description="Unique identifier for the article, inherited from RawArticle"
    )
    url: str = Field(
        ...,
        description="URL of the article, inherited from RawArticle"
    )
    title: str = Field(
        ...,
        description="Title of the article, inherited from RawArticle (may be cleaned)"
    )
    source_id: str = Field(
        ...,
        description="ID of the article's source, inherited from RawArticle as a string"
    )
    topic_id: str = Field(
        ...,
        description="ID of the article's topic, inherited from RawArticle as a string"
    )
    image: Optional[str] = Field(
        default=None,
        description="URL of the article's main image, inherited from RawArticle (optional, used for UI display)"
    )
    published_date: datetime = Field(
        ...,
        description="Date and time when the article was published, inherited from RawArticle"
    )
    content_length: int = Field(
        ...,
        description="Length of the cleaned content (in characters)"
    )
    content_clean: str = Field(
        ...,
        description="Cleaned content of the article, processed by Apache Spark"
    )
    alias: Optional[str] = Field(
        default=None,
        description="Alternative name or alias for the article, inherited from RawArticle (optional)"
    )
    crawl_timestamp: datetime = Field(
        ...,
        description="Timestamp when the article was crawled, inherited from RawArticle"
    )
    process_timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when the article was processed and cleaned"
    )
    language: str = Field(
        ...,
        description="Detected language of the cleaned content (e.g., 'en' for English)"
    )
    content_hash: str = Field(
        ...,
        description="MD5 hash of the cleaned content, used for deduplication"
    )
    metadata: dict = Field(
        default_factory=dict,
        description="Additional metadata for the processed article (e.g., processing details)"
    )

    # Configuration for Pydantic model
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            datetime: lambda v: v.isoformat()  # Convert datetime to ISO format for JSON/Parquet compatibility
        }
    )

    @field_validator("url", "image")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:  # Allow None for image
            return v
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        if not url_pattern.match(v):
            raise ValueError(f"Invalid URL format: {v}")
        return v

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        if len(v.strip()) < 1:
            raise ValueError("Title is too short (minimum 1 character)")
        return v.strip()

    @field_validator("content_clean")
    @classmethod
    def validate_content_clean(cls, v: str) -> str:
        if len(v) < 20:
            raise ValueError("Content is too short (minimum 20 characters)")
        return v

    @field_validator("published_date", "crawl_timestamp")
    @classmethod
    def validate_past_date(cls, v: datetime) -> datetime:
        if v > datetime.now():
            raise ValueError("Date cannot be in the future")
        return v

    @field_validator("content_length")
    @classmethod
    def validate_content_length(cls, v: int) -> int:
        if v < 20:
            raise ValueError("Content length must be at least 20 characters")
        return v

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        if not re.match(r'^[a-z]{2,3}$', v):
            raise ValueError("Language must be a 2 or 3-letter code (e.g., 'en', 'fr')")
        return v

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, v: str) -> str:
        if not re.match(r'^[a-f0-9]{32}$', v):
            raise ValueError("Content hash must be a 32-character MD5 hash")
        return v

    @classmethod
    def from_raw_article(cls, raw_article, cleaned_content: str):
        """Create a ProcessedArticle from a RawArticle and its cleaned content."""
        content_hash = hashlib.md5(cleaned_content.encode()).hexdigest()
        lang, _ = langid.classify(cleaned_content)
        
        return cls(
            article_id=raw_article.article_id,     # Inherit article_id from RawArticle
            url=raw_article.url,
            title=raw_article.title,
            source_id=str(raw_article.source_id),  # Convert ObjectId to string
            topic_id=str(raw_article.topic_id),    # Convert ObjectId to string
            image=raw_article.image,
            published_date=raw_article.published_date,
            content_length=len(cleaned_content),
            content_clean=cleaned_content,
            alias=raw_article.alias,
            crawl_timestamp=raw_article.crawl_timestamp,
            language=lang,
            content_hash=content_hash
        )