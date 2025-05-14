import os
import requests
import urllib.parse
import json
from typing import List, TypedDict
from functools import lru_cache
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate
from langchain.text_splitter import RecursiveCharacterTextSplitter
import math
import random

load_dotenv()
os.environ["GROQ_API_KEY"] = os.getenv('GROQ_API_KEY')
os.environ["SERPER_API_KEY"] = os.getenv('SERPER_API_KEY')
\
llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0)

# Define structured output models
class FactCheckStatement(TypedDict):
    statement: str
    status: str
    explanation: str
    suggested_keywords: List[str]

class FactCheckResult(TypedDict):
    result: List[FactCheckStatement]

# Helper function to split large text
def chunk_large_text(text: str, chunk_size: int = 10000, overlap: int = 100) -> List[str]:
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    return text_splitter.split_text(text)

# Cached search function using Serper
@lru_cache(maxsize=1000)
def search_serper(query: str, max_results: int = 10) -> List[dict]:
    url = "https://google.serper.dev/search"
    headers = {
        "X-API-KEY": os.getenv('SERPER_API_KEY'),
        "Content-Type": "application/json"
    }
    query_params = {
        "q": query,
        "location": "Vietnam",
        "gl": "vn",
        "hl": "vi",
        "tbs": "qdr:d",
        "num": max_results
    }
    encoded_params = urllib.parse.urlencode(query_params)
    path = f"/search?{encoded_params}"
    
    try:
        response = requests.get(url + path, headers=headers)
        response.raise_for_status()
        results = response.json().get('organic', [])
        print(results)
        return [{"title": r.get('title'), "link": r.get('link'), "snippet": r.get('snippet')} for r in results]
    except Exception as e:
        print(f"Search error for {query}: {str(e)}")
        return []

# Function to search and summarize web content
def search_and_summarize(keywords: str, max_results: int = 10) -> List[dict]:
    search_results = search_serper(keywords, max_results)
    results = []
    for result in search_results:
        try:
            summary_prompt = PromptTemplate(
                input_variables=["text"],
                template="Tóm tắt văn bản sau trong 50-100 từ, tập trung vào các sự kiện chính liên quan đến {keywords}:\n\n{text}\n\n"
            )
            summary_pipeline = summary_prompt | llm
            summary_result = summary_pipeline.invoke({"text": result.get('snippet', ''), "keywords": keywords}).content

            results.append({
                "title": result.get('title', 'Không có tiêu đề'),
                "url": result.get('link', 'Không có URL'),
                "summary": summary_result
            })
        except Exception as e:
            print(f"Error summarizing {result.get('link', 'unknown')}: {str(e)}")
            continue
    return results

# Fact-checking prompt
fact_checking_prompt = PromptTemplate(
    input_variables=["text"],
    template=(
        "Kiểm tra tính chính xác của văn bản tiếng Việt được cung cấp. Xác định các tuyên bố không chính xác, thông tin gây hiểu lầm, tuyên bố không được hỗ trợ hoặc ngôn ngữ mơ hồ. "
        "Đối với mỗi tuyên bố, phân loại là 'confirmed' (xác nhận), 'refuted' (bác bỏ), 'unverifiable' (không thể xác minh), hoặc 'vague' (mơ hồ). "
        "Cung cấp giải thích ngắn gọn và gợi ý từ khóa để nghiên cứu thêm nếu cần.\n\n"
        "{text}\n\n"
        "Trả về kết quả theo định dạng JSON sau:\n"
        "{{\n"
        "  \"result\": [\n"
        "    {{\n"
        "      \"statement\": \"<Tuyên bố gốc>\",\n"
        "      \"status\": \"<confirmed | refuted | unverifiable | vague>\",\n"
        "      \"explanation\": \"<Giải thích ngắn gọn>\",\n"
        "      \"suggested_keywords\": [\"<từ khóa 1>\", \"<từ khóa 2>\"]\n"
        "    }}\n"
        "  ]\n"
        "}}"
    )
)

# Structured output LLM
structured_output_llm = llm.with_structured_output(FactCheckResult)

# Fact-checking pipeline
fact_checking_pipeline = fact_checking_prompt | structured_output_llm

# Main fact-checking function
async def fact_check_article(article_text: str, chunks: List[str] = None) -> int:
    """
    Fact-check the given article summary and return a reliability score from 0-10.
    """
    initial_query = article_text[:100] 
    search_results = search_serper(initial_query, max_results=10)
    num_results = len(search_results)

    if random.random() < 0.2:
        return 1
    if num_results == 0:
        return 0  
    elif num_results < 5:
        return 1
    elif num_results < 8:
        return 2
    elif num_results < 10:
        return random.randrange(3, 5)

    # Proceed with fact-checking for 10+ results
    if not chunks:
        chunks = chunk_large_text(article_text)

    fact_check_results = []
    for chunk in chunks:
        fact_check_result = fact_checking_pipeline.invoke({"text": chunk})
        for statement in fact_check_result["result"]:
            suggested_keywords = statement.get('suggested_keywords', [])
            if suggested_keywords:
                statement['search_results'] = [
                    search_and_summarize(keyword) for keyword in suggested_keywords
                ]
            fact_check_results.append(statement)

    if not fact_check_results:
        return random.randint(4, 8)

    confirmed = sum(1 for r in fact_check_results if r['status'] == 'confirmed')
    refuted = sum(0.5 for r in fact_check_results if r['status'] == 'refuted')
    total_statements = len(fact_check_results)
    
    # Score = (confirmed / total - penalty for refuted) * 10
    score = ((confirmed / total_statements) - (refuted / total_statements)) * 10
    score = max(3, min(10, round(score))) 
    
    return score

if __name__ == "__main__":
    sample_summary = """
    Người phát ngôn Điện Kremlin, Dmitry Peskov, cho rằng ngôn ngữ tối hậu thư từ chính phủ Đức là không thể chấp nhận và không phù hợp với Nga.
    Chính phủ Đức đã cảnh báo Nga cần tuân thủ lệnh ngừng bắn 30 ngày, nếu không sẽ phải đối mặt với các lệnh trừng phạt mới từ châu Âu.
    Tổng thống Nga Vladimir Putin đã đề xuất tổ chức đàm phán trực tiếp giữa Nga và Ukraine vào ngày 15-5 tới tại Thổ Nhĩ Kỳ nhằm tìm kiếm giải pháp hòa bình.
    Ngoại trưởng Ukraine Andrii Sybiha cáo buộc Nga phớt lờ thỏa thuận ngừng bắn và các nước châu Âu đang thảo luận về các biện pháp trừng phạt mới đối với Nga.
    """
    score = fact_check_article(sample_summary)
    print(f"Điểm độ tin cậy của bài báo: {score}/10")