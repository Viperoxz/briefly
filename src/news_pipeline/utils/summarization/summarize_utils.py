import os
import asyncio
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from dagster import get_dagster_logger
import pandas as pd
from dotenv import load_dotenv
from bson import ObjectId

load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def summarize_article_async(content: str, client: AsyncOpenAI) -> str:
    """Asynchronously summarize an article using API with retry logic."""
    logger = get_dagster_logger()
    
    try:
        response = await client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "Bạn là một biên tập viên, hãy tóm tắt bài báo sau thành 4 ý chính bằng tiếng Việt. Viết trực tiếp các ý, không cần câu mở đầu, không đánh số, không sử dụng dấu gạch đầu dòng. Mỗi ý nên là một câu ngắn hoặc trung bình. Lưu ý: Xuống dòng sau mỗi ý. (Ví dụ: Ý 1\nÝ 2\nÝ 3\nÝ 4)"
                },
                {"role": "user", "content": content}
            ],
            model=os.getenv("OPENAI_MODEL_ID", "gpt-4o-mini"),
            temperature=0.4,
            max_tokens=225
        )
        
        summary = response.choices[0].message.content.strip()
        if not summary:
            raise ValueError("API returned empty summary")
        
        return summary
    except Exception as e:
        logger.error(f"Error during summarization: {e}")
        raise

async def process_articles_async(articles_df: pd.DataFrame, mongo_io_manager) -> pd.DataFrame:
    """Process all articles asynchronously."""
    logger = get_dagster_logger()
    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    tasks = []
    
    if 'summary' not in articles_df.columns:
        articles_df['summary'] = None
    else:
        articles_df['summary'] = articles_df['summary'].astype('object')

    for index, row in articles_df.iterrows():
        tasks.append(process_single_article(index, row, client, mongo_io_manager, articles_df))
    
    # Run all tasks concurrently and gather results
    await asyncio.gather(*tasks)
    
    return articles_df

async def process_single_article(index, row, client, mongo_io_manager, articles_df):
    """Process a single article asynchronously."""
    logger = get_dagster_logger()
    
    try:
        content = row["content"]
        summary = await summarize_article_async(content, client)
        summary_array = [item.strip() for item in summary.split('\n') if item.strip()]
        
        articles_df.at[index, "summary"] = pd.Series([summary_array], 
                                                    index=[index]).iloc[0]
        
        # Update MongoDB
        collection = mongo_io_manager._get_collection(None, "articles")
        collection.update_one(
            {"url": row["url"]},
            {"$set": {"summary": summary_array}},
            upsert=False
        )
        logger.info(f"✅ Summarized article: {row['url']}")
    except ValueError as ve:
        logger.error(f"❌ Value error for {row['url']}: {ve}")
    except Exception as e:
        logger.error(f"❌ Failed to summarize {row['url']}: {e}")