from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition
import pandas as pd
from ..utils.summarization.summarize_utils import summarize_content
from ..models.summarized_article import SummarizedArticle
from typing import Dict

# Định nghĩa phân vùng động cho bài báo
article_partitions_def = DynamicPartitionsDefinition(name="articles")

@asset(
    key="summarized_article",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    deps=["articles"]
)
def summarized_article(context, articles: pd.DataFrame) -> Output[Dict]:
    """Tóm tắt bài báo và cập nhật MongoDB."""
    logger = get_dagster_logger()
    article_id = context.partition_key

    # Tìm bài báo tương ứng với partition_key
    article = articles[articles['link'] == article_id]
    if article.empty:
        logger.error(f"Không tìm thấy bài báo với ID {article_id}")
        return Output(value={}, metadata={"num_articles": 0})

    article = article.iloc[0]
    try:
        article_model = SummarizedArticle(
            source=article["source"],
            topic=article["topic"],
            title=article["title"],
            link=article["link"],
            image=article.get("image", None),
            published=article["published"],
            content=article["content"],
            summary=""
        )

        # Tóm tắt nội dung
        summary = summarize_content(article_model.content)
        article_model.summary = summary

        # Cập nhật MongoDB ngay lập tức
        context.resources.mongo_io_manager.collection.update_one(
            {"link": article_model.link},
            {"$set": {"summary": summary}},
            upsert=True
        )
        logger.info(f"Đã tóm tắt bài báo {article_model.link}")

        return Output(
            value=article_model.model_dump(),
            metadata={"num_articles": 1}
        )

    except Exception as e:
        logger.error(f"Lỗi khi tóm tắt bài báo {article_id}: {e}")
        return Output(value={}, metadata={"num_articles": 0})