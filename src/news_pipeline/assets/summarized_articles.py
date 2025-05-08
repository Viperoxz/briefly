from dagster import asset, get_dagster_logger, Output, DynamicPartitionsDefinition, AssetIn
import pandas as pd
from ..utils.summarization.summarize_utils import summarize_content
from ..models.summarized_article import SummarizedArticle
from ..models.article import Article

# Định nghĩa phân vùng động cho bài báo
article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

@asset(
    key="summarized_articles",
    io_manager_key="mongo_io_manager",
    partitions_def=article_partitions_def,
    ins={"articles": AssetIn(key="articles")}
)
def summarized_articles(context, articles: pd.DataFrame) -> Output[pd.DataFrame]:
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
        summary = summarize_content(article_model.content)
        if not summary:
            logger.warning(f"Không tạo được tóm tắt cho {article_model.link}")
            return Output(value=pd.DataFrame(), metadata={"num_articles": 0})

        # Lưu tóm tắt vào MongoDB
        summarized_article = SummarizedArticle(**article_model.dict(), summary=summary)
        context.resources.mongo_io_manager.collection.update_one(
            {"link": summarized_article.link},
            {"$set": {"summary": summarized_article.summary}},
            upsert=True
        )

        logger.info(f"Đã tóm tắt bài báo {article_model.link}")
        return Output(
            value=pd.DataFrame([summarized_article.dict()]),
            metadata={"num_articles": 1}
        )

    except Exception as e:
        logger.error(f"Lỗi khi tóm tắt bài báo {article_id}: {e}")
        return Output(value=pd.DataFrame(), metadata={"num_articles": 0})