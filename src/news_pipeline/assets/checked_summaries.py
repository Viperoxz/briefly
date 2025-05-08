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

    # Sử dụng phương thức _get_collection để lấy collection
    try:
        collection = mongo_io_manager._get_collection(context)
        logger.info("Đã lấy collection từ MongoDB thành công")
    except Exception as e:
        logger.error(f"Lỗi khi lấy collection từ MongoDB: {e}")
        raise

    # Kiểm tra nếu DataFrame summarized_articles rỗng
    if summarized_articles.empty:
        logger.warning("DataFrame summarized_articles rỗng")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    # Lấy danh sách các bài báo đã tóm tắt từ DataFrame
    articles = summarized_articles.to_dict("records")
    logger.info(f"Đã nhận được {len(articles)} bài báo từ summarized_articles")

    if not articles:
        logger.warning("Không tìm thấy bài báo nào có tóm tắt")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    # Kiểm tra tóm tắt
    checked_articles = []
    for article in articles:
        try:
            if len(article.get("summary", "")) < 1:
                logger.warning(f"Tóm tắt quá ngắn cho bài báo {article.get('link', 'unknown')}")
                collection.update_one(
                    {"link": article["link"]},
                    {"$set": {"summary_status": "failed"}},
                    upsert=True
                )
            else:
                logger.info(f"Tóm tắt hợp lệ cho bài báo {article.get('link', 'unknown')}")
                collection.update_one(
                    {"link": article["link"]},
                    {"$set": {"summary_status": "passed"}},
                    upsert=True
                )
                checked_articles.append(article)
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra tóm tắt cho bài báo {article.get('link', 'unknown')}: {e}")
            continue

    if not checked_articles:
        logger.warning("Không có bài báo nào vượt qua kiểm tra tóm tắt")
        return Output(value=pd.DataFrame(), metadata={"num_checked": 0})

    df = pd.DataFrame(checked_articles)
    logger.info(f"Đã kiểm tra {len(df)} bài báo")
    return Output(
        value=df,
        metadata={"num_checked": len(df)}
    )