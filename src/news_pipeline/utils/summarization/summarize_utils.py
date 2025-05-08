import asyncio
from groq import Groq
from tenacity import retry, stop_after_attempt, wait_fixed
from pymongo import MongoClient
from datetime import datetime
import os
from typing import List
from ...config import settings

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
async def summarize_content_async(content: str) -> str:
    """Summarize content asynchronously with retry logic."""
    client = Groq(api_key=settings.GROQ_API_KEY)
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: client.chat.completions.create(
                model=settings.GROQ_MODEL_ID,
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
            raise ValueError("Summary is empty")
        return summary
    except Exception as e:
        # Lưu lỗi vào collection riêng
        mongo_client = MongoClient(os.getenv("MONGO_URI"))
        db = mongo_client[os.getenv("MONGO_DB")]
        db["failed_summaries"].insert_one({
            "content": content,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
        raise Exception(f"Failed to summarize content: {e}")

def summarize_content(content: str) -> str:
    """Synchronous wrapper for summarize_content_async."""
    return asyncio.run(summarize_content_async(content))

async def summarize_multiple_contents(contents: List[str]) -> List[str]:
    """Summarize multiple contents concurrently."""
    tasks = [summarize_content_async(content) for content in contents]
    return await asyncio.gather(*tasks, return_exceptions=True)