from qdrant_client import QdrantClient
import os
from dotenv import load_dotenv

# Tải biến môi trường từ file .env
load_dotenv()

# Cấu hình Qdrant client
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = "ArticleEmbeddings"  

# Khởi tạo Qdrant client
client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
)

try:
    # Lấy thông tin về collection
    collection_info = client.get_collection(collection_name=COLLECTION_NAME)
    
    # Số lượng point trong collection
    points_count = collection_info.points_count
    
    print(f"Số lượng point trong collection '{COLLECTION_NAME}': {points_count}")

except Exception as e:
    print(f"Đã xảy ra lỗi khi truy vấn Qdrant: {e}")