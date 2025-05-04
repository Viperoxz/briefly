from groq import Groq
import os
from dotenv import load_dotenv
from dagster import get_dagster_logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import asyncio
import aiohttp
import redis

load_dotenv()


@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=4, max=10),  # Exponential backoff
    retry=retry_if_exception_type(Exception)  # Retry on any exception (customize for 429 if needed)
)
def summarize_content(content: str, max_length: int = 512) -> str:
    logger = get_dagster_logger()
    api_key = os.getenv("GROQ_API_KEY")
    model_id = os.getenv("GROQ_MODEL_ID")  
    
    if not api_key:
        logger.error("GROQ_API_KEY is not set in the environment variables.")
        return ""
    if not model_id:
        logger.error("GROQ_MODEL_ID is not set in the environment variables.")
        return ""

    try:
        client = Groq(api_key=api_key)
        prompt = (
        "Đây là một bài báo tin tức bằng tiếng Việt. Hãy tóm tắt nội dung thành 4 ý chính dưới dạng dấu đầu dòng, "
        "tập trung vào các thông tin quan trọng và khách quan. Không tóm tắt quá ngắn (ít nhất 20 kí tự). **Không sinh ra câu giới thiệu hay bất kỳ câu thừa nào**.\n\n"
        f"{content}")

        response = client.chat.completions.create(
            model=model_id,  
            messages=[
                {"role": "system", "content": "Bạn là một trợ lý thông minh, giỏi tóm tắt văn bản tiếng Việt một cách rõ ràng."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_length,
            temperature=0.5,
        )
        
        summary = response.choices[0].message.content.strip()
        logger.info(f"Summarize successfully: ({len(summary.split())} words)")
        time.sleep(1)  
        return summary
    
    except Exception as e:
        logger.error(f"Error when summarizing: {e}")
        return ""
