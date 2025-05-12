import re

def clean_summary(summary_text):
    """Remove introductory phrases from summaries."""
    patterns = [
        r"^Here are the \d+ concise points in Vietnamese:[\s\n]*",
        r"^Here are \d+ concise points:[\s\n]*",
        r"^Summary in \d+ points:[\s\n]*",
        r"^Key points:[\s\n]*"
    ]
    
    cleaned_text = summary_text
    for pattern in patterns:
        cleaned_text = re.sub(pattern, "", cleaned_text, flags=re.IGNORECASE)
    
    # Loại bỏ bullets và các ký tự đánh dấu
    cleaned_text = re.sub(r"^[•\-\*\–\—]\s*", "", cleaned_text, flags=re.MULTILINE)
    
    # Loại bỏ khoảng trắng thừa
    cleaned_text = re.sub(r"\n{2,}", "\n", cleaned_text)
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

text = """
• Vụ tai nạn liên hoàn xảy ra tại đường Kim Giang, xã Thanh Liệt, huyện Thanh Trì vào khoảng 21h đêm 9-5.
• Ô tô mang biển kiểm soát 30A-017.XX đã tông 6 xe máy, khiến 3 người bị thương, trong đó 2 người phải cấp cứu tại Bệnh viện Bạch Mai và tài xế ô tô có dấu hiệu đã sử dụng bia, rượu.
• Cơ quan chức năng đang tập trung xác minh, làm rõ người lái ô tô để lập hồ sơ xử lý vụ việc.
• Theo báo cáo, chủ ô tô là Đ.H.A., ngụ quận Ba Đình
"""

cleaned_text = clean_summary(text)
print(cleaned_text)