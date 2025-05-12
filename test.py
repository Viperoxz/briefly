import os
import asyncio
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv(override=True)

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
content = """
TP HuếHỏa pháo súng thần công bắn trên Kỳ Đài gặp sự cố, tàn lửa rơi trên đầu khán giả do hết hạn sử dụng, tồn từ dịp Festival nghề 2023. Ngày 10/5, Trung tâm Bảo tồn Di tích cố đô Huế đã gửi văn bản cho Sở Văn hóa Thể thao và Công an TP Huế giải trình việc hỏa pháo súng thần công trên Kỳ Đài Huế gặp sự cố vào tối 26/4 và 3/5.Ông Lê Công Sơn, Phó giám đốc Trung tâm Bảo tồn di tích cố đô Huế, cho biết sự cố do hiệu ứng hỏa thuật thần công không đảm bảo yêu cầu kỹ thuật. Do chưa kịp đặt hàng vật tư hỏa thuật của Công ty Z21, nhân viên kỹ thuật của Công ty TNHH Việt Giang, đơn vị chịu trách nhiệm kỹ thuật, tự ý sử dụng 58 ống hỏa thuật rồng lửa cũ sót lại ở Festival nghề truyền thống Huế 2023."Các ống hỏa thuật này đã bị thấm nước do thời gian lắp đặt gặp mưa gió và hết hạn sử dụng, dẫn đến chất lượng không đảm bảo. Ngoài ra, do sơ suất trong khâu lắp đặt, hướng hỏa thuật bị lắp ngang mặt đất khiến hiệu ứng bắn lệch về phía khán giả", ông Sơn nói. Tàn lửa của hỏa pháo rơi trên đầu khán giả đêm 3/5. Ảnh: Đắc Thành Sau sự cố, Công ty TNHH Việt Giang đã nhận trách nhiệm, cam kết chỉ sử dụng vật tư hỏa thuật của Công ty Z21 (không sử dụng pháo nổ) còn hạn sử dụng, bảo đảm đúng tiêu chuẩn kỹ thuật và lắp đặt đúng quy trình để không tái diễn các sự cố tương tự trong các lần trình diễn tiếp theo.Theo kế hoạch, 19h15 thứ bảy hàng tuần, Trung tâm Bảo tồn di tích cố đô Huế bắn hỏa pháo súng thần công trên Kỳ Đài để tạo điểm nhấn, tạo không khí vui tươi cho người dân và du khách. Kỳ Đài bố trí 8 khẩu súng thần công, mỗi khẩu bắn tối đa 9 phát, mỗi đêm bắn 72 phát, trong 40 giây. Loại hỏa pháo mới thay thế cho hệ thống súng thần công phun dầu cũ.Trung tâm Bảo tồn di tích cố đô Huế tổ chức bắn hỏa pháo đêm 26/4 và 3/5 song đều gặp sự cố. Tối 3/5, hàng nghìn người dân và du khách đang xem dưới khu vực Phu Văn Lâu, cách nơi đặt hỏa pháo khoảng 50 m, phải bỏ chạy khi tàn lửa đỏ rực rơi trên đầu.Sau sự cố, Cục Văn hóa cơ sở, Gia đình và Thư viện của Bộ Văn hóa Thể thao và Du lịch đã yêu cầu TP Huế kiểm tra việc tổ chức bắn hỏa pháo súng thần công tại khu vực Kỳ Đài - quảng trường Ngọ Môn. Sở Văn hóa và Thể thao TP Huế được giao xác minh sự việc theo chỉ đạo từ Cục.Võ Thạnh Ngày 10/5, Trung tâm Bảo tồn Di tích cố đô Huế đã gửi văn bản cho Sở Văn hóa Thể thao và Công an TP Huế giải trình việc hỏa pháo súng thần công trên Kỳ Đài Huế gặp sự cố vào tối 26/4 và 3/5. Ông Lê Công Sơn, Phó giám đốc Trung tâm Bảo tồn di tích cố đô Huế, cho biết sự cố do hiệu ứng hỏa thuật thần công không đảm bảo yêu cầu kỹ thuật. Do chưa kịp đặt hàng vật tư hỏa thuật của Công ty Z21, nhân viên kỹ thuật của Công ty TNHH Việt Giang, đơn vị chịu trách nhiệm kỹ thuật, tự ý sử dụng 58 ống hỏa thuật rồng lửa cũ sót lại ở Festival nghề truyền thống Huế 2023. "Các ống hỏa thuật này đã bị thấm nước do thời gian lắp đặt gặp mưa gió và hết hạn sử dụng, dẫn đến chất lượng không đảm bảo. Ngoài ra, do sơ suất trong khâu lắp đặt, hướng hỏa thuật bị lắp ngang mặt đất khiến hiệu ứng bắn lệch về phía khán giả", ông Sơn nói. Tàn lửa của hỏa pháo rơi trên đầu khán giả đêm 3/5. Ảnh: Đắc Thành Sau sự cố, Công ty TNHH Việt Giang đã nhận trách nhiệm, cam kết chỉ sử dụng vật tư hỏa thuật của Công ty Z21 (không sử dụng pháo nổ) còn hạn sử dụng, bảo đảm đúng tiêu chuẩn kỹ thuật và lắp đặt đúng quy trình để không tái diễn các sự cố tương tự trong các lần trình diễn tiếp theo. Theo kế hoạch, 19h15 thứ bảy hàng tuần, Trung tâm Bảo tồn di tích cố đô Huế bắn hỏa pháo súng thần công trên Kỳ Đài để tạo điểm nhấn, tạo không khí vui tươi cho người dân và du khách. Kỳ Đài bố trí 8 khẩu súng thần công, mỗi khẩu bắn tối đa 9 phát, mỗi đêm bắn 72 phát, trong 40 giây. Loại hỏa pháo mới thay thế cho hệ thống súng thần công phun dầu cũ. Trung tâm Bảo tồn di tích cố đô Huế tổ chức bắn hỏa pháo đêm 26/4 và 3/5 song đều gặp sự cố. Tối 3/5, hàng nghìn người dân và du khách đang xem dưới khu vực Phu Văn Lâu, cách nơi đặt hỏa pháo khoảng 50 m, phải bỏ chạy khi tàn lửa đỏ rực rơi trên đầu. Sau sự cố, Cục Văn hóa cơ sở, Gia đình và Thư viện của Bộ Văn hóa Thể thao và Du lịch đã yêu cầu TP Huế kiểm tra việc tổ chức bắn hỏa pháo súng thần công tại khu vực Kỳ Đài - quảng trường Ngọ Môn. Sở Văn hóa và Thể thao TP Huế được giao xác minh sự việc theo chỉ đạo từ Cục. Võ Thạnh
"""

async def generate_summary():
    response = await client.chat.completions.create(
        model="gpt-4o-mini",  
        messages=[
            {
                "role": "assistant",
                "content": "Bạn là một biên tập viên, hãy tóm tắt bài báo sau thành 4 ý chính bằng tiếng Việt. Viết trực tiếp các ý, không cần câu mở đầu, không đánh số, không sử dụng dấu gạch đầu dòng. Mỗi ý nên là một câu ngắn hoặc trung bình. Lưu ý: Xuống dòng sau mỗi ý. (Ví dụ: Ý 1\nÝ 2\nÝ 3\nÝ 4)"
            },
            {
                "role": "user",
                "content": content
            }
        ],
        temperature=0.4,
        max_tokens=225
    )
    
    summary = response.choices[0].message.content.strip()
    print(summary)
    summary_array = [item.strip() for item in summary.split('\n') if item.strip()]
    print("Summary Array:", len(summary_array))
    return summary_array

# Run the async function
asyncio.run(generate_summary())

# from pymongo import MongoClient

# def clear_mongodb_database():
#     # Connect to MongoDB
#     client = MongoClient("mongodb://localhost:27017/")
    
#     # Access the snap_news database
#     db = client["snap_news"]
    
#     # Get all collection names
#     collections = db.list_collection_names()
    
#     # Drop each collection
#     for collection in collections:
#         db[collection].drop()
#         print(f"Dropped collection: {collection}")
    
#     print(f"All collections in 'snap_news' database have been deleted")
    
#     # Close the connection
#     client.close()

# if __name__ == "__main__":
#     clear_mongodb_database()

# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv
# import pandas as pd

# load_dotenv()

# # Kết nối MongoDB
# client = MongoClient(os.getenv("MONGO_URI"))
# db = client[os.getenv("MONGO_DB")]
# collection = db["articles"]

# # Kiểm tra bài báo cụ thể
# url = "https://tienphong.vn/can-benh-tu-than-dua-den-vien-thuong-da-muon-dau-hieu-canh-bao-chi-dau-dau-te-bi-chan-tay-post1740676.tpo"
# article = collection.find_one({"url": url})

# print("Bài báo có trong MongoDB:", bool(article))
# print("Trạng thái summary:", "summary" in article and bool(article["summary"]))

# # Tạo DataFrame từ bài báo và thực hiện test_tts thủ công
# if article and "summary" in article:
#     df = pd.DataFrame([article])
#     print("DataFrame:", df.shape)
#     print("Các cột:", df.columns.tolist())
#     print("URL đầu tiên:", df["url"].iloc[0])
#     print("Nội dung summary:", df["summary"].iloc[0][:100] + "...")