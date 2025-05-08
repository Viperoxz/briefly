import asyncio
from groq import Groq
from tenacity import retry, stop_after_attempt, wait_fixed
from pymongo import MongoClient
from datetime import datetime
import os
from typing import List
from ...config import settings
from dagster import get_dagster_logger
from dotenv import load_dotenv

load_dotenv()

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
async def summarize_content_async(content: str, mongo_client: MongoClient = None) -> str:
    """Summarize content asynchronously with retry logic."""
    logger = get_dagster_logger()
    
    # Kiểm tra nội dung đầu vào
    if not content or content.strip() == "":
        logger.error("Nội dung đầu vào rỗng hoặc không hợp lệ")
        raise ValueError("Nội dung đầu vào rỗng hoặc không hợp lệ")

    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    if not client:
        logger.error("Không thể khởi tạo client Groq")
        raise ValueError("Không thể khởi tạo client Groq")
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: client.chat.completions.create(
                model=os.getenv("GROQ_MODEL_ID"),
                messages=[
                    {
                        "role": "system",
                        "content": "Summarize the following news article into 4 concise points in Vietnamese. Each point should be a short or medium sentence."
                    },
                    {"role": "user", "content": content}
                ],
                temperature=0.7,
                max_tokens=150
            )
        )
        summary = response.choices[0].message.content.strip()
        if not summary:
            logger.error("API Groq trả về tóm tắt rỗng")
            raise ValueError("Tóm tắt rỗng từ API Groq")
        
        logger.info(f"Tóm tắt được tạo: {summary[:50]}...")
        return summary

    except Exception as e:
        # Lưu lỗi vào collection riêng
        if mongo_client is None:
            mongo_client = MongoClient(os.getenv("MONGO_URI"))
        db = mongo_client[os.getenv("MONGO_DB")]
        db["failed_summaries"].insert_one({
            "content": content[:500],  # Giới hạn độ dài để tránh lưu quá nhiều dữ liệu
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
        logger.error(f"Không thể tóm tắt nội dung: {e}")
        raise ValueError(f"Không thể tóm tắt nội dung: {e}")

def summarize_content(content: str, mongo_client: MongoClient = None) -> str:
    """Synchronous wrapper for summarize_content_async."""
    logger = get_dagster_logger()
    try:
        return asyncio.run(summarize_content_async(content, mongo_client))
    except ValueError as e:
        logger.error(f"Lỗi khi tóm tắt nội dung: {e}")
        raise
    except Exception as e:
        logger.error(f"Lỗi không xác định khi tóm tắt nội dung: {e}")
        raise ValueError(f"Lỗi không xác định khi tóm tắt nội dung: {e}")

async def summarize_multiple_contents(contents: List[str], mongo_client: MongoClient = None) -> List[str]:
    """Summarize multiple contents concurrently."""
    logger = get_dagster_logger()
    tasks = [summarize_content_async(content, mongo_client) for content in contents if content and content.strip()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Lỗi khi tóm tắt nội dung {i}: {result}")
    return results