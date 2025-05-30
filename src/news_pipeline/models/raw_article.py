from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator
from typing import Optional
from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv
import re
import hashlib

load_dotenv()

class RawArticle(BaseModel):
    article_id: str = Field(
        ...,
        description="Generated from source_id, published_date and url hash"
    )
    url: str = Field(
        ...,
        description="URL of the article, used to access the original article content"
    )
    title: str = Field(
        ...,
        description="Title of the article, extracted from the source"
    )
    source_id: ObjectId = Field(
        ...,
        description="ID of the article's source, referencing the Sources collection in MongoDB"
    )
    topic_id: ObjectId = Field(
        ...,
        description="ID of the article's topic, referencing the Topics collection in MongoDB"
    )
    image: Optional[str] = Field(
        default=None,
        description="URL of the article's main image, used for display purposes in UI (optional)"
    )
    published_date: datetime = Field(
        ...,
        description="Date and time when the article was published by the source"
    )
    content: str = Field(
        ...,
        description="Raw content of the article, extracted during the crawling process"
    )
    crawl_timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when the article was crawled and stored"
    )
    metadata: dict = Field(
        default_factory=dict,
        description="Additional metadata for the article (e.g., author, tags), stored as key-value pairs"
    )

    @model_validator(mode="before")
    @classmethod
    def generate_article_id(cls, values):
        if "article_id" not in values or not values.get("article_id"):
            source_id = str(values.get("source_id", "unknown"))
            published_date = values.get("published_date")
            url = values.get("url", "")

            date_str = (
                published_date.strftime("%Y%m%d%H%M%S")
                if isinstance(published_date, datetime)
                else "00000000000000"
            )

            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

            values["article_id"] = f"{source_id}_{date_str}_{url_hash}"

        return values

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            ObjectId: lambda v: str(v),  # Convert ObjectId to string for JSON serialization
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

    @field_validator("content")
    @classmethod
    def validate_content(cls, v: str) -> str:
        if len(v) < 20:
            raise ValueError("Content is too short (minimum 20 characters)")
        return v

    @field_validator("published_date")
    @classmethod
    def validate_published_date(cls, v: datetime) -> datetime:
        if v > datetime.now():
            raise ValueError("Published date cannot be in the future")
        return v
    

if __name__ == "__main__":
    
    article = RawArticle(
        url="https://example.com/news/article-123",
        title="AI is transforming research",
        source_id=ObjectId("662f0c0be1d3f2c8a01ab7e9"),
        topic_id=ObjectId("662f0c0be1d3f2c8a01ab7e1"),
        published_date=datetime(2025, 5, 30, 12, 0),
        content="hé lô xin chào tất cả các bạn",
    )
    print(article.model_dump())