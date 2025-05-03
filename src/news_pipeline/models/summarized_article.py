from pydantic import Field, field_validator
from .article import Article

class SummarizedArticle(Article):
    summary: str = Field(..., description="Summarized content of the article")

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, v):
        if len(v) < 20:
            raise ValueError("Summary is too short (minimum 20 characters)")
        return v
