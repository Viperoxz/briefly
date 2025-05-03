import pytest
from ..news_pipeline.assets.articles import extract_full_article

# Use a real article URL that is likely to remain stable
SAMPLE_ARTICLE_URL = "https://www.bbc.com/news/world-us-canada-68748038"


def test_extract_full_article_returns_text():
    """Test that the function returns non-empty text from a valid URL."""
    content = extract_full_article(SAMPLE_ARTICLE_URL)
    assert isinstance(content, str), "Output should be a string"
    assert len(content) > 100, "Extracted content should be reasonably long"


def test_extract_full_article_invalid_url():
    """Test that the function handles invalid URLs gracefully."""
    bad_url = "https://thisdoesnotexist.example.com/article"
    content = extract_full_article(bad_url)
    assert content == "", "Should return empty string on failure"


def test_extract_full_article_bad_format():
    """Test with a URL that returns non-HTML content (like an image or PDF)."""
    weird_url = "https://example.com/image.png"
    content = extract_full_article(weird_url)
    assert isinstance(content, str), "Should return string"
    assert content == "" or len(content) < 50, "Should not extract much from binary content"


@pytest.mark.timeout(10)
def test_timeout_behavior():
    """Ensure requests don't hang."""
    slow_url = "http://10.255.255.1"  # non-routable IP to simulate hang
    content = extract_full_article(slow_url)
    assert content == "", "Should return empty string on timeout"
