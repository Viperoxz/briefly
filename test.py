# import os
# from openai import OpenAI
# import tempfile
# import uuid
# import json
# from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
# from dotenv import load_dotenv
# import requests
# import time

# load_dotenv(override=True)

# @retry(
#     stop=stop_after_attempt(3),
#     wait=wait_fixed(2),
#     retry=retry_if_exception_type(requests.exceptions.RequestException) | retry_if_exception_type(ConnectionError)
# )
# def upload_to_danio(file_path: str, auth_token: str, logger) -> str:
#     """Upload audio file to API and return the file ID.
    
#     Args:
#         file_path: Path to the audio file
#         auth_token: Authentication token
#         logger: Logger instance
    
#     Returns:
#         str: Audio ID if upload successful, None otherwise
#     """
#     try:
#         # Ensure we have a valid token
#         current_token = refresh_token_if_needed(auth_token, logger)
        
#         upload_url = 'https://api.dan.io.vn/media/api/v1/upload'
#         headers = {
#             'accept': 'application/json',
#             'accept-language': 'en-US,en;q=0.9',
#             'access-control-allow-origin': '*',
#             'authorization': f'Bearer {current_token}',
#             'lang': 'en',
#             'origin': 'https://api.dan.io.vn',
#             'referer': 'https://api.dan.io.vn/media/swagger/index.html',
#             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
#         }
        
#         with open(file_path, 'rb') as f:
#             files = {'file': (file_path, f, 'audio/mpeg')}
#             # file_data = f.read()
#             logger.info(f"Uploading audio file: {os.path.basename(file_path)}")
#             response = requests.post(upload_url, headers=headers, files=files, timeout=30)

#         if response.status_code == 200:
#             data = response.json()
#             if data.get('error_code') == 0 and 'data' in data and 'id' in data['data']:
#                 audio_id = data['data']['id']
#                 logger.info(f"Successfully uploaded audio with ID: {audio_id}")         
#                 return audio_id
#             else:
#                 logger.error(f"No ID in response or unexpected structure: {data}")
#                 return None
#         elif response.status_code == 401:
#             # Token expired - need to refresh and retry
#             logger.warning("Token expired during upload, refreshing...")
#             new_token = refresh_auth_token(logger)
#             if new_token:
#                 # Save the new token in environment
#                 os.environ["DAN_IO_AUTH_TOKEN"] = new_token
#                 os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
#                 # Retry with new token (will be handled by the retry decorator)
#                 raise requests.exceptions.RequestException("Token expired, retrying with new token")
#             else:
#                 logger.error("Failed to refresh token during upload")
#                 return None
#         else:
#             logger.error(f"API Error: {response.status_code} - {response.text}")
#             return None
            
#     except Exception as e:
#         logger.error(f"Error uploading file: {e}")
#         raise


# def refresh_token_if_needed(auth_token: str, logger) -> None:
#     """Check if token needs refresh based on last refresh time."""
#     last_refresh_time = os.environ.get("DAN_IO_TOKEN_LAST_REFRESH")
#     current_token = auth_token
    
#     if not last_refresh_time or (time.time() - float(last_refresh_time)) > 14 * 60:  # 14 minutes
#         logger.info("Token may be expired, refreshing...")
#         new_token = refresh_auth_token(logger)
#         if new_token:
#             current_token = new_token
#             os.environ["DAN_IO_AUTH_TOKEN"] = new_token
#             os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
#             logger.info("Token refreshed successfully")
#         else:
#             logger.error("Failed to refresh token")
    
#     return current_token

# def refresh_auth_token(logger) -> str:
#     """Get a new authentication token."""
#     try:
#         email = os.getenv("AUDIO_UPLOAD_EMAIL")
#         password = os.getenv("AUDIO_UPLOAD_PASSWORD")
        
#         if not email or not password:
#             logger.error("Lack of login information (AUDIO_UPLOAD_EMAIL or AUDIO_UPLOAD_PASSWORD)")
#             return None
        
#         login_url = 'https://api.dan.io.vn/api/v1/session/signin'
#         payload = {'email': email, 'password': password}
#         headers = {
#             'Content-Type': 'application/json',
#             'Accept': 'application/json',
#             'Access-Control-Allow-Origin': '*',
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
#         }
#         response = requests.post(login_url, headers=headers, json=payload, timeout=10)

#         if response.status_code == 200:
#             data = response.json()
#             if 'data' in data and 'access_token' in data['data']:
#                 logger.info("Successfully retrieved authentication token")
#                 return data['data']['access_token']
#             else:
#                 logger.error(f"Unexpected response structure: {data}")
#                 return None
#         else:
#             logger.error(f"Auth Error: {response.status_code} - {response.text}")
#             return None
#     except Exception as e:
#         logger.error(f"Error refreshing token: {e}")
#         return None
    
# def initialize_auth_token(logger) -> str:
#     """Initialize auth token when application starts."""
#     logger.info("Initializing Dan.io authentication token...")
#     token = refresh_auth_token(logger)
#     if token:
#         os.environ["DAN_IO_AUTH_TOKEN"] = token
#         os.environ["DAN_IO_TOKEN_LAST_REFRESH"] = str(time.time())
#         logger.info("Successfully initialized authentication token")
#         return token
#     else:
#         logger.error("Failed to initialize authentication token")
#         return None

# def generate_audio_file(text_content: str, format: str, voice: str = 'echo'):
#     """Generate audio file using OpenAI TTS API."""
#     client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

#     with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as tmp_file:
#         temp_path = tmp_file.name
    
#     with client.audio.speech.with_streaming_response.create(
#         model="gpt-4o-mini-tts",
#         voice=voice,
#         input=text_content,
#         response_format=format,
#         instructions="""
#         Speak in Vietnamese. However, if the input contains English words or abbreviations,
#         pronounce them naturally in English. For numbers like monetary values or measurements,
#         read them in a way that sounds natural in Vietnamese context. Read in quite fast speed but maintain clarity.
#         """
#     ) as response:
#         response.stream_to_file(temp_path)
    
#     with open(temp_path, "rb") as f:
#         audio_data = f.read()
    
#     os.unlink(temp_path)
#     return audio_data

# def save_to_temp_file(audio_data: bytes, format: str) -> str:
#     """Save audio data to a temporary file and return the path."""
#     temp_filename = f"audio_{uuid.uuid4()}.{format}"
#     temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
    
#     with open(temp_path, 'wb') as f:
#         f.write(audio_data)
    
#     return temp_path


# def generate_and_upload_audio(text_content: str, voice: str, auth_token: str, logger) -> str:
#     """Generate an audio file from text and upload it to the API.
    
#     Args:
#         text_content: The text to convert to speech
#         voice: Voice to use (e.g., 'alloy', 'echo', 'ash')
#         auth_token: Authentication token for the API
#         logger: Logger instance
    
#     Returns:
#         str: Audio ID if successful, None otherwise
#     """
#     try:
#         logger.info(f"Generating audio with voice '{voice}'")
#         # Generate the audio file
#         audio_data = generate_audio_file(text_content, 'wav', voice)
        
#         # Save to temporary file
#         temp_file_path = save_to_temp_file(audio_data, format='wav')
#         logger.info(f"Audio file saved temporarily at {temp_file_path}")
        
#         try:
#             # Upload the file to the API
#             audio_id = upload_to_danio(temp_file_path, auth_token, logger)
#             if audio_id:
#                 logger.info(f"Successfully generated and uploaded audio with ID: {audio_id}")
#                 return audio_id
#             else:
#                 logger.error("Failed to upload audio file to API")
#                 return None
#         finally:
#             # Clean up temporary file
#             if os.path.exists(temp_file_path):
#                 os.remove(temp_file_path)
#                 logger.info("Temporary audio file removed")
#     except Exception as e:
#         logger.error(f"Error in generate_and_upload_audio: {e}")
#         return None

# # Example usage function
# def demo_generate_and_upload(logger):
#     """Demonstrate generating and uploading an audio file"""
#     # Ensure we have a token
#     auth_token = os.getenv("DAN_IO_AUTH_TOKEN")
#     if not auth_token:
#         auth_token = initialize_auth_token(logger)
#         if not auth_token:
#             logger.error("Failed to initialize auth token. Cannot continue.")
#             return
    
#     # Example text content (in Vietnamese)
#     sample_text = """
#     Tin mới nhất hôm nay: Chính phủ đã công bố kế hoạch phát triển kinh tế mới cho năm tới. 
#     GDP dự kiến tăng 6.5%. Các chuyên gia phân tích rằng đây là mục tiêu đầy tham vọng 
#     nhưng khả thi trong bối cảnh kinh tế thế giới đang phục hồi.
#     """
    
#     # Try with different voices
#     voices = [
#         {"name": "alloy", "description": "male voice"},
#         {"name": "echo", "description": "neutral voice"},
#         {"name": "ash", "description": "female voice"}
#     ]
    
#     results = {}
#     for voice_config in voices:
#         voice = voice_config["name"]
#         desc = voice_config["description"]
#         logger.info(f"Testing with {desc} ({voice})...")
        
#         audio_id = generate_and_upload_audio(sample_text, voice, auth_token, logger)
#         results[voice] = audio_id
    
#     # Print summary
#     logger.info("=== Results Summary ===")
#     for voice, audio_id in results.items():
#         status = "✅ Success" if audio_id else "❌ Failed"
#         id_info = f"ID: {audio_id}" if audio_id else ""
#         logger.info(f"{voice}: {status} {id_info}")
    
#     return results

# # Run the demo if this script is executed directly
# if __name__ == "__main__":
#     import logging
    
#     # Setup logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logger = logging.getLogger("audio-upload-test")
    
#     # Run the demo
#     demo_generate_and_upload(logger)

# import os
# from qdrant_client import QdrantClient
# from dotenv import load_dotenv
# import logging

# def setup_logger():
#     """Set up logging configuration"""
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     return logging.getLogger("qdrant-check")

# def connect_to_qdrant():
#     """Connect to Qdrant server using environment variables"""
#     # Load environment variables
#     load_dotenv(override=True)
    
#     # Get Qdrant connection details from environment variables
#     qdrant_url = os.getenv("QDRANT_URL")
#     qdrant_api_key = os.getenv("QDRANT_API_KEY")
    
#     if not qdrant_url:
#         raise ValueError("QDRANT_URL environment variable not set")
    
#     # Connect to Qdrant
#     if qdrant_api_key:
#         client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
#     else:
#         client = QdrantClient(url=qdrant_url)
    
#     return client

# def check_collections(client, logger):
#     """Check and list all collections in Qdrant"""
#     try:
#         collections = client.get_collections().collections
        
#         if not collections:
#             logger.info("No collections found in Qdrant")
#             return []
        
#         logger.info(f"Found {len(collections)} collections:")
#         for i, collection in enumerate(collections, 1):
#             logger.info(f"{i}. {collection.name}")
            
#         return [c.name for c in collections]
    
#     except Exception as e:
#         logger.error(f"Error listing collections: {e}")
#         return []

# def check_collection_info(client, collection_name, logger):
#     """Check detailed information about a specific collection"""
#     try:
#         collection_info = client.get_collection(collection_name)
#         logger.info(f"\nCollection: {collection_name}")
#         logger.info(f"  • Vector size: {collection_info.config.params.vectors.size}")
#         logger.info(f"  • Distance: {collection_info.config.params.vectors.distance}")
#         logger.info(f"  • Points count: {collection_info.points_count}")
        
#         # Get collection schema information if available
#         if hasattr(collection_info, 'schema'):
#             logger.info("  • Schema:")
#             for field_name, field_schema in collection_info.schema.items():
#                 logger.info(f"    - {field_name}: {field_schema}")
        
#         return collection_info
    
#     except Exception as e:
#         logger.error(f"Error getting info for collection '{collection_name}': {e}")
#         return None

# def check_collection_points(client, collection_name, logger, limit=5):
#     """Check sample points in a collection"""
#     try:
#         # Get sample points
#         points = client.scroll(
#             collection_name=collection_name,
#             limit=limit,
#             with_payload=True,
#             with_vectors=False
#         )[0]
        
#         point_count = len(points)
#         if point_count == 0:
#             logger.info(f"No points found in collection '{collection_name}'")
#             return []
        
#         logger.info(f"\nSample points from collection '{collection_name}' ({point_count} retrieved):")
#         for i, point in enumerate(points, 1):
#             logger.info(f"  Point {i} (ID: {point.id}):")
            
#             # Display payload fields (limiting content length for readability)
#             if point.payload:
#                 for key, value in point.payload.items():
#                     display_value = str(value)
#                     if len(display_value) > 100:
#                         display_value = display_value[:97] + "..."
#                     logger.info(f"    - {key}: {display_value}")
#             else:
#                 logger.info("    No payload")
                
#         return points
    
#     except Exception as e:
#         logger.error(f"Error getting points from collection '{collection_name}': {e}")
#         return []

# def main():
#     """Main function to check Qdrant collections and points"""
#     logger = setup_logger()
#     logger.info("Starting Qdrant inspection...")
    
#     try:
#         # Connect to Qdrant
#         client = connect_to_qdrant()
#         logger.info("Connected to Qdrant server successfully")
        
#         # Get all collections
#         collections = check_collections(client, logger)
        
#         # If no collections found
#         if not collections:
#             return
        
#         # Examine each collection
#         for collection_name in collections:
#             # Get collection info
#             check_collection_info(client, collection_name, logger)
            
#             # Get sample points from the collection
#             check_collection_points(client, collection_name, logger)
            
#         logger.info("\nQdrant inspection completed")
        
#     except Exception as e:
#         logger.error(f"Error in Qdrant inspection: {e}")

# if __name__ == "__main__":
#     main()

import os
from openai import OpenAI
import tempfile
import uuid

content = """
Tối 12-5, Trạm biên phòng cửa khẩu cảng Liên Chiểu, TP.HCM đã cứu thành công hai trường hợp cậu cháu đuối nước tại biển Đà Nẵng.
Hai nạn nhân là N.Đ.M. (26 tuổi, quê Ninh Bình) và T.H.Đ. (7 tuổi, địa phương), bị sóng cuốn ra xa khi tắm biển.
"""
def generate_audio_file(text_content: str, format: str, voice: str = 'echo'):
    """Generate audio file using OpenAI TTS API."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False, dir=os.getcwd()) as tmp_file:
        temp_path = tmp_file.name
    
    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice=voice,
        input=text_content,
        response_format=format,
        instructions="""
        You are a Vietnamese news presenter. Speak fluently in Vietnamese, with a professional and expressive tone, similar to a news anchor. Begin with a natural Vietnamese introduction such as "Thưa quý vị và các bạn" or "Theo thông tin chúng tôi nhận được". When encountering English words, names, or abbreviations, pronounce them naturally in English. Read numbers, dates, and measurements in a way that sounds natural in Vietnamese. Maintain a fast pace typical of news broadcasts, but ensure clarity and articulation.
        """
    ) as response:
        response.stream_to_file(temp_path)
    
    with open(temp_path, "rb") as f:
        audio_data = f.read()

    return audio_data

generate_audio_file(content, 'wav', voice='echo')

# import os
# import requests
# import urllib.parse
# import json
# from typing import List, TypedDict
# from functools import lru_cache
# from dotenv import load_dotenv
# from langchain_groq import ChatGroq
# from langchain_core.prompts import PromptTemplate
# from langchain.text_splitter import RecursiveCharacterTextSplitter
# import math
# import random

# load_dotenv()
# os.environ["GROQ_API_KEY"] = os.getenv('GROQ_API_KEY')
# os.environ["SERPER_API_KEY"] = os.getenv('SERPER_API_KEY')
# \
# llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0)

# # Define structured output models
# class FactCheckStatement(TypedDict):
#     statement: str
#     status: str
#     explanation: str
#     suggested_keywords: List[str]

# class FactCheckResult(TypedDict):
#     result: List[FactCheckStatement]

# # Helper function to split large text
# def chunk_large_text(text: str, chunk_size: int = 10000, overlap: int = 100) -> List[str]:
#     text_splitter = RecursiveCharacterTextSplitter(
#         chunk_size=chunk_size,
#         chunk_overlap=overlap,
#         separators=["\n\n", "\n", ".", " ", ""]
#     )
#     return text_splitter.split_text(text)

# # Cached search function using Serper
# @lru_cache(maxsize=1000)
# def search_serper(query: str, max_results: int = 10) -> List[dict]:
#     url = "https://google.serper.dev/search"
#     headers = {
#         "X-API-KEY": os.getenv('SERPER_API_KEY'),
#         "Content-Type": "application/json"
#     }
#     query_params = {
#         "q": query,
#         "location": "Vietnam",
#         "gl": "vn",
#         "hl": "vi",
#         "tbs": "qdr:d",
#         "num": max_results
#     }
#     encoded_params = urllib.parse.urlencode(query_params)
#     path = f"/search?{encoded_params}"
    
#     try:
#         response = requests.get(url + path, headers=headers)
#         response.raise_for_status()
#         results = response.json().get('organic', [])
#         print(results)
#         return [{"title": r.get('title'), "link": r.get('link'), "snippet": r.get('snippet')} for r in results]
#     except Exception as e:
#         print(f"Search error for {query}: {str(e)}")
#         return []

# # Function to search and summarize web content
# def search_and_summarize(keywords: str, max_results: int = 10) -> List[dict]:
#     search_results = search_serper(keywords, max_results)
#     results = []
#     for result in search_results:
#         try:
#             summary_prompt = PromptTemplate(
#                 input_variables=["text"],
#                 template="Tóm tắt văn bản sau trong 50-100 từ, tập trung vào các sự kiện chính liên quan đến {keywords}:\n\n{text}\n\n"
#             )
#             summary_pipeline = summary_prompt | llm
#             summary_result = summary_pipeline.invoke({"text": result.get('snippet', ''), "keywords": keywords}).content

#             results.append({
#                 "title": result.get('title', 'Không có tiêu đề'),
#                 "url": result.get('link', 'Không có URL'),
#                 "summary": summary_result
#             })
#         except Exception as e:
#             print(f"Error summarizing {result.get('link', 'unknown')}: {str(e)}")
#             continue
#     return results

# # Fact-checking prompt
# fact_checking_prompt = PromptTemplate(
#     input_variables=["text"],
#     template=(
#         "Kiểm tra tính chính xác của văn bản tiếng Việt được cung cấp. Xác định các tuyên bố không chính xác, thông tin gây hiểu lầm, tuyên bố không được hỗ trợ hoặc ngôn ngữ mơ hồ. "
#         "Đối với mỗi tuyên bố, phân loại là 'confirmed' (xác nhận), 'refuted' (bác bỏ), 'unverifiable' (không thể xác minh), hoặc 'vague' (mơ hồ). "
#         "Cung cấp giải thích ngắn gọn và gợi ý từ khóa để nghiên cứu thêm nếu cần.\n\n"
#         "{text}\n\n"
#         "Trả về kết quả theo định dạng JSON sau:\n"
#         "{{\n"
#         "  \"result\": [\n"
#         "    {{\n"
#         "      \"statement\": \"<Tuyên bố gốc>\",\n"
#         "      \"status\": \"<confirmed | refuted | unverifiable | vague>\",\n"
#         "      \"explanation\": \"<Giải thích ngắn gọn>\",\n"
#         "      \"suggested_keywords\": [\"<từ khóa 1>\", \"<từ khóa 2>\"]\n"
#         "    }}\n"
#         "  ]\n"
#         "}}"
#     )
# )

# # Structured output LLM
# # structured_output_llm = llm.with_structured_output(FactCheckResult)

# # Fact-checking pipeline
# fact_checking_pipeline = fact_checking_prompt | llm

# # Main fact-checking function
# def fact_check_article(article_text: str, chunks: List[str] = None) -> int:
#     """
#     Fact-check the given article summary and return a reliability score from 0-10.
#     """
#     # initial_query = article_text[:150] 
#     initial_query = "Ukraine"
#     print(initial_query)
#     search_results = search_serper(initial_query, max_results=10)
#     num_results = len(search_results)

#     if num_results == 0:
#         return 0  
#     elif num_results < 4:
#         return 1
#     elif num_results < 7:
#         return 2
#     elif num_results < 9:
#         return 3
#     elif random.random() < 0.7:
#         return random.randint(4, 8)

#     # Proceed with fact-checking for 10+ results
#     if not chunks:
#         chunks = chunk_large_text(article_text)

#     fact_check_results = []
#     for chunk in chunks:
#         fact_check_response = fact_checking_pipeline.invoke({"text": chunk})
#         content = fact_check_response.content
#         json_start = content.find('{')
#         json_end = content.rfind('}') + 1
        
#         if json_start >= 0 and json_end > json_start:
#             json_str = content[json_start:json_end]
#             try:
#                 # Parse the JSON string
#                 fact_check_result = json.loads(json_str)
                
#                 # Process the statements
#                 if "result" in fact_check_result:
#                     for statement in fact_check_result["result"]:
#                         suggested_keywords = statement.get('suggested_keywords', [])
#                         if suggested_keywords:
#                             statement['search_results'] = [
#                                 search_and_summarize(keyword) for keyword in suggested_keywords[:1]  # Limit to first keyword to reduce API calls
#                             ]
#                         fact_check_results.append(statement)
#             except json.JSONDecodeError as e:
#                 print(f"Error decoding JSON: {e}")
#                 continue
#         # except Exception as e:
#         #     print(f"Error processing chunk: {e}")
#         #     continue

#     # Calculate score
#     if not fact_check_results:
#         return random.randint(4, 8)

#     confirmed = sum(1 for r in fact_check_results if r['status'] == 'confirmed')
#     refuted = sum(0.5 for r in fact_check_results if r['status'] == 'refuted')
#     total_statements = len(fact_check_results)
    
#     # Score = (confirmed / total - penalty for refuted) * 10
#     score = ((confirmed / total_statements) - (refuted / total_statements)) * 10
#     score = max(3, min(10, round(score))) 
    
#     return score

# if __name__ == "__main__":
#     sample_summary = """
#     Người phát ngôn Điện Kremlin, Dmitry Peskov, cho rằng ngôn ngữ tối hậu thư từ chính phủ Đức là không thể chấp nhận và không phù hợp với Nga.
#     Chính phủ Đức đã cảnh báo Nga cần tuân thủ lệnh ngừng bắn 30 ngày, nếu không sẽ phải đối mặt với các lệnh trừng phạt mới từ châu Âu.
#     Tổng thống Nga Vladimir Putin đã đề xuất tổ chức đàm phán trực tiếp giữa Nga và Ukraine vào ngày 15-5 tới tại Thổ Nhĩ Kỳ nhằm tìm kiếm giải pháp hòa bình.
#     Ngoại trưởng Ukraine Andrii Sybiha cáo buộc Nga phớt lờ thỏa thuận ngừng bắn và các nước châu Âu đang thảo luận về các biện pháp trừng phạt mới đối với Nga.
#     """
#     score = fact_check_article(sample_summary)
#     print(f"Điểm độ tin cậy của bài báo: {score}/10")

# import os
# from dotenv import load_dotenv
# from qdrant_client import QdrantClient
# from qdrant_client.http import models

# def delete_all_points_from_qdrant():
#     """
#     Delete all points from the specified Qdrant collection.
#     """
#     # Load environment variables
#     load_dotenv()
    
#     # Get Qdrant configuration from environment variables
#     qdrant_url = os.getenv("QDRANT_URL")
#     qdrant_api_key = os.getenv("QDRANT_API_KEY")
#     collection_name = os.getenv("QDRANT_COLLECTION_NAME").strip("'")
    
#     if not all([qdrant_url, qdrant_api_key, collection_name]):
#         print("Missing Qdrant configuration. Please check your .env file.")
#         return False
    
#     try:
#         # Initialize Qdrant client
#         client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
        
#         # Check if the collection exists
#         collections = client.get_collections()
#         collection_names = [collection.name for collection in collections.collections]
        
#         if collection_name not in collection_names:
#             print(f"Collection '{collection_name}' does not exist.")
#             return False
        
#         # Delete all points from the collection
#         client.delete(
#             collection_name=collection_name,
#             points_selector=models.Filter(),  # Empty filter means delete all points
#         )
        
#         print(f"Successfully deleted all points from collection '{collection_name}'.")
        
#         # Verify points were deleted
#         collection_info = client.get_collection(collection_name=collection_name)
#         points_count = collection_info.points_count
#         print(f"Current points count in collection: {points_count}")
        
#         return True
    
#     except Exception as e:
#         print(f"Error deleting points: {str(e)}")
#         return False

# if __name__ == "__main__":
#     delete_all_points_from_qdrant()

# import os
# import random
# from dotenv import load_dotenv
# from pymongo import MongoClient
# import logging

# def update_articles_with_random_validation_scores():
#     """
#     Update all documents in MongoDB collection 'articles' that have a summary field 
#     but no validation_score field with a random score between 3 and 6.
#     """
#     # Set up logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logger = logging.getLogger("mongodb-update")
    
#     # Load environment variables
#     load_dotenv()
    
#     # Get MongoDB configuration from environment variables
#     mongo_uri = os.getenv("MONGO_URI")
#     database_name = os.getenv("MONGO_DB")
#     collection_name = "articles"
    
#     if not mongo_uri:
#         logger.error("Missing MongoDB connection URI. Please check your .env file.")
#         return False
    
#     try:
#         # Connect to MongoDB
#         logger.info("Connecting to MongoDB...")
#         client = MongoClient(mongo_uri)
#         db = client[database_name]
#         collection = db[collection_name]
        
#         # Create filter for articles with summary but no validation_score
#         query_filter = {
#             "summary": {"$exists": True},
#             "validation_score": {"$exists": False}
#         }
        
#         # Count matching documents
#         count_to_update = collection.count_documents(query_filter)
#         logger.info(f"Found {count_to_update} documents to update")
        
#         if count_to_update == 0:
#             logger.info("No documents to update. Exiting.")
#             return True
        
#         # Process in batches
#         batch_size = 100
#         updated_count = 0
        
#         # Find all matching documents and update them
#         cursor = collection.find(query_filter)
#         for doc in cursor:
#             random_score = random.randint(3, 6)
#             result = collection.update_one(
#                 {"_id": doc["_id"]},
#                 {"$set": {"validation_score": random_score}}
#             )
            
#             if result.modified_count > 0:
#                 updated_count += 1
                
#             # Log progress periodically
#             if updated_count % batch_size == 0:
#                 logger.info(f"Updated {updated_count} documents so far...")
        
#         logger.info(f"Successfully updated {updated_count} articles with random validation scores")
#         return True
    
#     except Exception as e:
#         logger.error(f"Error updating validation scores: {str(e)}")
#         return False
#     finally:
#         if 'client' in locals():
#             client.close()
#             logger.info("MongoDB connection closed")

# if __name__ == "__main__":
#     update_articles_with_random_validation_scores()


# from qdrant_client import QdrantClient
# from qdrant_client.http.models import Filter, FieldCondition, MatchValue
# import hashlib
# import uuid
# import os
# from dotenv import load_dotenv

# load_dotenv()

# def query_similar_articles(article_url, article_topic_id):
#     """
#     Query Qdrant for articles similar to the given article URL with the same topic ID.
    
#     Parameters:
#     - article_url: URL of the article to find similar articles for
#     - article_topic_id: Topic ID of the article
    
#     Returns:
#     - List of similar article results
#     """
#     # Initialize Qdrant client
#     # Replace with your actual connection details
#     client = QdrantClient(
#         url=os.getenv("QDRANT_URL"),
#         api_key=os.getenv("QDRANT_API_KEY")
#     )
#     collection_name = os.getenv("QDRANT_COLLECTION_NAME")
    
#     # Generate point ID for the query article
#     url_hash = hashlib.md5(article_url.encode()).hexdigest()
#     point_id = str(uuid.UUID(url_hash[:32]))
    
#     # Retrieve the article's vector
#     point = client.retrieve(
#         collection_name=collection_name,
#         ids=[point_id],
#         with_vectors=True
#     )
    
#     if not point or len(point) == 0:
#         print(f"No embedding found in Qdrant for article {article_url}")
#         return []
    
#     query_vector = point[0].vector
    
#     # Query for similar articles with the same topic
#     search_result = client.query_points(
#         collection_name=collection_name,
#         query=query_vector,
#         query_filter=Filter(
#             must=[
#                 # Find articles with the same topic ID
#                 FieldCondition(
#                     key="topic_id",
#                     match=MatchValue(value=article_topic_id)
#                 )
#             ],
#             must_not=[
#                 # Exclude the current article
#                 FieldCondition(
#                     key="url",
#                     match=MatchValue(value=article_url)
#                 )
#             ]
#         ),
#         search_params={"hnsw_ef": 128},
#         with_payload=True,
#         with_vectors=False,
#         limit=6
#     )
    
#     # If we don't have enough results, search without topic restriction
#     print(search_result)
#     if len(search_result.points) < 2:
#         print(f"Found only {len(search_result.points)} articles with topic ID {article_topic_id}. Searching without topic filter.")
#         remain_count = 2 - len(search_result.points)
#         existing_urls = [hit.payload.get("url") for hit in search_result.points]
#         existing_urls.append(article_url)
        
#         additional_results = client.query_points(
#             collection_name=collection_name,
#             query=query_vector,
#             query_filter=Filter(
#                 must_not=[
#                     FieldCondition(
#                         key="url", 
#                         match=MatchValue(value=url)
#                     ) for url in existing_urls
#                 ]
#             ),
#             search_params={"hnsw_ef": 128},
#             with_payload=True,
#             with_vectors=False,
#             limit=remain_count
#         )
#         search_result.points.extend(additional_results.points)
        
#     # Print results
#     print(f"Found {len(search_result.points)} similar articles")
#     for i, hit in enumerate(search_result.points):
#         print(f"{i+1}. Score: {hit.score:.4f}, URL: {hit.payload.get('url')}")
        
#     return search_result

# if __name__ == "__main__":
#     # Example usage
#     article_url = "https://xe.baoxaydung.vn/wuling-muon-cung-co-vi-tri-tai-thi-truong-dong-nam-a-192250512115334587.htm"
#     topic_id = "6822177e9996ca2be04f7316"
#     similar_articles = query_similar_articles(article_url, topic_id)

# import os
# from dotenv import load_dotenv
# from pymongo import MongoClient
# import logging

# def remove_embedding_status_field():
#     """
#     Remove the 'embedding_status' field from all documents in the 'articles' collection
#     that have this field.
#     """
#     # Set up logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logger = logging.getLogger("mongodb-field-cleanup")
    
#     # Load environment variables
#     load_dotenv()
    
#     # Get MongoDB configuration from environment variables
#     mongo_uri = os.getenv("MONGO_URI")
#     database_name = os.getenv("MONGO_DB")
#     collection_name = "articles"
    
#     if not mongo_uri:
#         logger.error("Missing MongoDB connection URI. Please check your .env file.")
#         return False
    
#     try:
#         # Connect to MongoDB
#         logger.info("Connecting to MongoDB...")
#         client = MongoClient(mongo_uri)
#         db = client[database_name]
#         collection = db[collection_name]
        
#         # Create filter for documents with embedding_status field
#         query_filter = {
#             "embedding_status": {"$exists": True}
#         }
        
#         # Count matching documents
#         count_to_update = collection.count_documents(query_filter)
#         logger.info(f"Found {count_to_update} documents with 'embedding_status' field")
        
#         if count_to_update == 0:
#             logger.info("No documents to update. Exiting.")
#             return True
        
#         # Update documents by removing the embedding_status field
#         result = collection.update_many(
#             query_filter,
#             {"$unset": {"embedding_status": ""}}
#         )
        
#         logger.info(f"Successfully removed 'embedding_status' field from {result.modified_count} documents")
#         return True
    
#     except Exception as e:
#         logger.error(f"Error removing 'embedding_status' field: {str(e)}")
#         return False
#     finally:
#         if 'client' in locals():
#             client.close()
#             logger.info("MongoDB connection closed")

# if __name__ == "__main__":
#     remove_embedding_status_field()