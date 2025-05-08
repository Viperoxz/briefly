from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
from ..utils.summarization import summarize_content
from ..models.summarized_article import SummarizedArticle
from ..models.article import Article
import pandas as pd
from typing import Dict

# Định nghĩa phân vùng động cho bài báo
article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="summarized_articles",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    ins={"articles": AssetIn(key="articles")}
)
def summarized_articles(context, articles: pd.DataFrame) -> Output[pd.DataFrame]:
    """Summarize bài báo từ raw content và lưu vào MongoDB."""
    logger = get_dagster_logger()
    article_id = context.partition_key

    # Lấy bài báo từ articles
    article = articles[articles['link'] == article_id]
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

        # Tóm tắt nội dung
        try:
            # Truyền mongo_client từ resources
            summary = summarize_content(
                article_model.content,
                mongo_client=context.resources.mongo_io_manager.client
            )
        except ValueError as e:
            logger.error(f"Không tạo được tóm tắt cho {article_model.link}: {e}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "error": str(e)})

        summarized_article = SummarizedArticle(
            **article_model.dict(), summary=summary
        )

        # Lưu vào MongoDB
        context.resources.mongo_io_manager.collection.update_one(
            {"link": article_model.link},
            {"$set": summarized_article.dict()},
            upsert=True
        )

        logger.info(f"Đã tóm tắt và lưu bài báo {article_model.link} với summary: {summary[:50]}...")
        df = pd.DataFrame([summarized_article.dict()])
        logger.info(f"Returning DataFrame: {df}")

        return Output(
            value=df,
            metadata={"num_articles": 1}
        )

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bài báo {article_id}: {e}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0, "error": str(e)})