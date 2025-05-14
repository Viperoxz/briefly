import pytest
from src.news_pipeline.utils.embedding import clean_text, chunk_text, generate_embedding

def test_clean_text():
    text = "Đây là câu. Đây là câu. Ảnh: Hoài Thanh"
    cleaned = clean_text(text)
    assert cleaned == "Đây là câu. "
    assert "Ảnh: Hoài Thanh" not in cleaned

def test_chunk_text():
    text = "Đây là câu đầu tiên. Đây là câu thứ hai. Đây là câu thứ ba."
    chunks = chunk_text(text)
    assert len(chunks) >= 1
    assert all(len(chunk) > 10 for chunk in chunks)

def test_generate_embedding():
    texts = [["Đây là chunk 1"], ["Đây là chunk 2"]]
    embeddings = generate_embedding(texts)
    assert len(embeddings) == 2
    assert all(len(emb) == 1 for emb in embeddings)
    assert all(len(emb[0]) == 768 for emb in embeddings if emb[0])  