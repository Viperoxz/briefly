from dagster import asset, get_dagster_logger, Output, AssetIn
import pandas as pd

@asset(
    key="checked_summaries",
    io_manager_key="mongo_io_manager",
    ins={"summarized_articles": AssetIn(key="summarized_articles")}
)
def checked_summaries(context, summarized_articles: pd.DataFrame):
    logger = get_dagster_logger()
    mongo_io_manager = context.resources.mongo_io_manager

    try:
        collection = mongo_io_manager._get_collection(context)
        logger.info("Successfully retrieved collection from MongoDB")
    except Exception as e:
        logger.error(f"Failed to get collection from MongoDB: {e}")
        raise

    # Kiểm tra nếu DataFrame summarized_articles rỗng
    if summarized_articles.empty:
        logger.warning("DataFrame summarized_articles is empty")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    # Lấy danh sách các bài báo đã tóm tắt từ DataFrame
    articles = summarized_articles.to_dict("records")
    logger.info(f"Found {len(articles)} articles in summarized_articles")

    if not articles:
        logger.warning("No articles found with summary")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    # Kiểm tra tóm tắt
    checked_articles = []
    for article in articles:
        try:
            if len(article.get("summary", "")) < 1:
                logger.warning(f"Summary too short for article {article.get('url', 'unknown')}")
                collection.update_one(
                    {"url": article["url"]},
                    {"$set": {"summary_status": "failed"}},
                    upsert=True
                )
            else:
                logger.info(f"Summary valid for article {article.get('url', 'unknown')}")
                collection.update_one(
                    {"url": article["url"]},
                    {"$set": {"summary_status": "passed"}},
                    upsert=True
                )
                checked_articles.append(article)
        except Exception as e:
            logger.error(f"Error checking summary for article {article.get('url', 'unknown')}: {e}")
            continue

    if not checked_articles:
        logger.warning("No articles passed summary check")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    df = pd.DataFrame(checked_articles)
    logger.info(f"Checked {len(df)} articles")
    return Output(
        value=df,
        metadata={"num_checked": len(df)}
    )