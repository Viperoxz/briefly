from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from tenacity import retry, stop_after_attempt, wait_fixed
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.embedded_article import EmbeddedArticle
from ..models.article import Article
import pandas as pd
from typing import Dict

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def ensure_mongo_record(mongo_io_manager, article_model):
    """Ensure article exists in MongoDB with retry logic."""
    logger = get_dagster_logger()
    doc = mongo_io_manager.collection.find_one({"link": article_model.link})
    if not doc:
        mongo_io_manager.collection.insert_one(article_model.dict())
        logger.info(f"Đã chèn bài báo {article_model.link} vào MongoDB")
    return True

@asset(
    key="embedded_articles",
    io_manager_key="mongo_io_manager",  # Sử dụng mongo_io_manager thay vì qdrant_io_manager
    partitions_def=article_partitions_def,
    ins={"raw_articles": AssetIn(key="articles")}
)
def embedded_articles(context, raw_articles: pd.DataFrame) -> Output[pd.DataFrame]:
    """Embed bài báo từ raw content, lưu vào MongoDB và Qdrant."""
    logger = get_dagster_logger()
    article_id = context.partition_key

    # Lấy bài báo từ raw_articles
    article = raw_articles[raw_articles['link'] == article_id]
    if article.empty:
        logger.error(f"Không tìm thấy bài báo với ID {article_id}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

    article = article.iloc[0]
    try:
        article_model = Article(
            source=article["source"],
            topic=article["topic"],
            title=article["title"],
            link=article["link"],
            image=article.get("image", None),
            published=article["published"],
            content=article["content"]
        )

        # Đảm bảo bài báo có trong MongoDB
        ensure_mongo_record(context.resources.mongo_io_manager, article_model)

        # Làm sạch và chia nhỏ nội dung
        cleaned_content = clean_text(article_model.content)
        chunks = chunk_text(cleaned_content)
        if not chunks:
            logger.warning(f"Không tạo được đoạn nào cho {article_model.link}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

        # Tạo embedding
        embeddings = generate_embedding([chunks])[0]
        if not embeddings:
            logger.warning(f"Không tạo được embedding cho {article_model.link}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

        # Lưu embedding vào Qdrant
        context.resources.qdrant_io_manager.store_embedding(
            point_id=str(article_model.link),
            vector=embeddings[0],  # Lấy embedding đầu tiên
            payload={
                "source": article_model.source,
                "topic": article_model.topic,
                "title": article_model.title,
            }
        )

        # Lưu embeddings vào MongoDB
        context.resources.mongo_io_manager.collection.update_one(
            {"link": article_model.link},
            {"$set": {"embeddings": embeddings}},
            upsert=True
        )

        logger.info(f"Đã tạo và lưu embedding cho bài báo {article_model.link}")
        embedded_article = EmbeddedArticle(**article_model.dict(), embeddings=embeddings)
        
        # Chuyển đổi thành DataFrame
        df = pd.DataFrame([embedded_article.dict()])
        logger.info(f"Returning DataFrame: {df}")

        return Output(
            value=df,
            metadata={
                "num_articles": 1,
                "sample_embedding": embeddings[0][:5] if embeddings else None
            }
        )

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bài báo {article_id}: {e}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})