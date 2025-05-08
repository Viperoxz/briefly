# import json
# from pathlib import Path
# from typing import Dict
# from dagster import asset

# @asset
# def rss_feed_list() -> Dict:
#     """Load RSS feed list from JSON config file."""
#     base_dir = Path(__file__).resolve().parent.parent.parent.parent
#     file_path = base_dir / "config" / "rss_feeds.json"
    
#     with open(file_path, "r", encoding="utf-8") as f:
#         rss_sources = json.load(f)
#     return rss_sources
from dagster import asset, get_dagster_logger
import json
from pathlib import Path
import os

@asset(key="rss_feed_list")
def rss_feed_list(context):
    logger = get_dagster_logger()
    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    config_path = base_dir / "config" / "rss_feeds.json"
    
    try:
        logger.info(f"Đang đọc file RSS feeds từ: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            feeds = json.load(f)
        logger.info(f"Đã đọc thành công RSS feeds: {feeds}")
        
        if not feeds:
            logger.warning("Danh sách RSS feeds rỗng")
            return {}
        
        return feeds
    
    except FileNotFoundError as e:
        logger.error(f"Không tìm thấy file {config_path}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi khi đọc file JSON {config_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Lỗi không xác định khi đọc RSS feeds: {e}")
        raise