import re
from bs4 import BeautifulSoup

def clean_article_content(content):
    """
    Clean article content by removing HTML, extra whitespace, and normalizing text
    """
    if not content:
        return ""
    
    # Remove HTML tags
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text()
    
    # Remove multiple spaces, newlines, tabs
    text = re.sub(r'\s+', ' ', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Remove common boilerplate/ads phrases (customize as needed)
    patterns_to_remove = [
        r'Đăng ký nhận tin',
        r'Theo dõi chúng tôi trên',
        r'Copyright © \d{4}',
        r'Tất cả các quyền được bảo lưu',
        r'Xem thêm:.*?$',
        r'Nguồn:.*?$'
    ]
    
    for pattern in patterns_to_remove:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    return text