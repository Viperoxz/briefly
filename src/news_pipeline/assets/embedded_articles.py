from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
import pandas as pd
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from typing import Dict, List

# Định nghĩa phân vùng động cho bài báo
article_partitions_def = DynamicPartitionsDefinition(name="articles")

@asset(
    key="embedded_articles",
    io_manager_key="qdrant_io_manager",
    partitions_def=article_partitions_def,
    ins={"summarized_article": AssetIn(key="summarized_article")}
)
def embedded_articles(context, summarized_article: Dict) -> Output[Dict]:
    """Embed bài báo đã tóm tắt và lưu vào Qdrant."""
    logger = get_dagster_logger()
    article_id = context.partition_key

    try:
        article_model = EmbeddedArticle(**summarized_article)
        if not article_model.summary:
            raise ValueError(f"Bài báo {article_model.link} thiếu tóm tắt")

        # Kiểm tra xem bài báo có tồn tại trong MongoDB không
        mongo_io_manager = context.resources.mongo_io_manager
        doc = mongo_io_manager.collection.find_one({"link": article_model.link})
        if not doc:
            mongo_io_manager.collection.insert_one(article_model.dict())
            logger.info(f"Đã chèn bài báo {article_model.link} vào MongoDB")
        elif not doc.get("summary"):
            mongo_io_manager.collection.update_one(
                {"link": article_model.link},
                {"$set": {"summary": article_model.summary}},
                upsert=True
            )
            logger.info(f"Đã cập nhật tóm tắt cho bài báo {article_model.link} trong MongoDB")

        # Làm sạch, chia nhỏ và tạo embedding
        cleaned_content = clean_text(article_model.content)
        chunks = chunk_text(cleaned_content)
        if not chunks:
            logger.warning(f"Không tạo được đoạn nào cho {article_model.link}")
            return Output(value=article_model.dict(), metadata={"num_articles": 0})

        embeddings = generate_embedding([chunks])[0]
        article_model.embeddings = embeddings

        logger.info(f"Đã tạo embedding cho bài báo {article_model.link}")
        return Output(
            value=article_model.dict(),
            metadata={
                "num_articles": 1,
                "sample_embedding": embeddings[0][:5] if embeddings else None
            }
        )

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bài báo {article_id}: {e}")
        return Output(value={}, metadata={"num_articles": 0})