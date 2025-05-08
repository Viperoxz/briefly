from dagster import multi_asset, AssetOut, Output, get_dagster_logger, DynamicPartitionsDefinition, AssetIn
import pandas as pd
from ..utils.embedding import clean_text, chunk_text, generate_embedding
from ..models.summarized_article import SummarizedArticle
from typing import Dict

@multi_asset(
    outs={
        "synced_articles": AssetOut(io_manager_key="mongo_io_manager"),
        "qdrant_embeddings": AssetOut(io_manager_key="qdrant_io_manager"),
    },
    partitions_def=DynamicPartitionsDefinition(name="article_partitions"),
    ins={
        "summarized_articles": AssetIn(key="summarized_articles"),
        "embedded_articles": AssetIn(key="embedded_articles"),
    },
)
def synced_articles(context, summarized_articles: pd.DataFrame, embedded_articles: pd.DataFrame):
    """Synchronize MongoDB and Qdrant after summarization and embedding."""
    logger = get_dagster_logger()
    partition_key = context.partition_key

    # Log để debug kiểu dữ liệu của input
    logger.info(f"Type of embedded_articles: {type(embedded_articles)}, Content: {embedded_articles}")
    logger.info(f"Type of summarized_articles: {type(summarized_articles)}, Content: {summarized_articles}")

    # Check summarized article
    summarized_article = summarized_articles[summarized_articles["link"] == partition_key]
    if summarized_article.empty:
        logger.warning(f"Không tìm thấy bài báo tóm tắt cho {partition_key}")
        yield Output(
            value=pd.DataFrame(),
            output_name="synced_articles",
            metadata={"status": "skipped"}
        )
        yield Output(
            value=None,
            output_name="qdrant_embeddings",
            metadata={"status": "skipped"}
        )
        return

    summarized_article = summarized_article.iloc[0]
    article_dict = summarized_article.to_dict()

    # Kiểm tra xem có trường summary không
    if "summary" not in article_dict or not article_dict["summary"]:
        logger.error(f"Bài báo {partition_key} thiếu trường summary")
        yield Output(
            value=pd.DataFrame(),
            output_name="synced_articles",
            metadata={"status": "skipped", "error": "Missing summary"}
        )
        yield Output(
            value=None,
            output_name="qdrant_embeddings",
            metadata={"status": "skipped"}
        )
        return

    try:
        article_model = SummarizedArticle(**article_dict)
    except Exception as e:
        logger.error(f"Lỗi khi tạo SummarizedArticle cho {partition_key}: {e}")
        yield Output(
            value=pd.DataFrame(),
            output_name="synced_articles",
            metadata={"status": "skipped", "error": str(e)}
        )
        yield Output(
            value=None,
            output_name="qdrant_embeddings",
            metadata={"status": "skipped"}
        )
        return

    # Check embedding in Qdrant
    qdrant_points = context.resources.qdrant_io_manager.load_input(context)
    qdrant_ids = {point.id for point in qdrant_points}
    if partition_key not in qdrant_ids:
        logger.warning(f"Không tìm thấy embedding cho {partition_key} trong Qdrant")
        try:
            cleaned_content = clean_text(article_model.content)
            chunks = chunk_text(cleaned_content)
            embeddings = generate_embedding([chunks])[0]
            context.resources.qdrant_io_manager.store_embedding(
                point_id=str(article_model.link),
                vector=embeddings[0],
                payload={
                    "source": article_model.source,
                    "topic": article_model.topic,
                    "title": article_model.title,
                }
            )
            logger.info(f"Tạo lại embedding cho {partition_key} trong Qdrant")

            # Lưu embeddings vào MongoDB để đảm bảo đồng bộ
            context.resources.mongo_io_manager.collection.update_one(
                {"link": article_model.link},
                {"$set": {"embeddings": embeddings}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Lỗi khi tạo embedding cho {partition_key}: {e}")

    # Ensure summary in MongoDB
    context.resources.mongo_io_manager.collection.update_one(
        {"link": article_model.link},
        {"$set": {"summary": article_model.summary}},
        upsert=True
    )

    # Remove orphaned Qdrant embeddings
    mongo_links = set(summarized_articles["link"])
    for point in qdrant_points:
        if point.id not in mongo_links:
            context.resources.qdrant_io_manager.client.delete(
                collection_name=context.resources.qdrant_io_manager.collection_name,
                points_selector=[point.id]
            )
            logger.info(f"Xóa embedding không đồng bộ: {point.id}")

    # Yield final outputs
    yield Output(
        value=pd.DataFrame([article_model.dict()]),
        output_name="synced_articles",
        metadata={"num_articles": 1}
    )
    yield Output(
        value=None,
        output_name="qdrant_embeddings",
        metadata={"num_synced": 1}
    )