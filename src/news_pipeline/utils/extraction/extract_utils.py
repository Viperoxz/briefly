import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import http.client
import urllib.error
import socket
import feedparser

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type((
        http.client.RemoteDisconnected, 
        urllib.error.URLError, 
        socket.timeout,
        requests.exceptions.RequestException,
        ConnectionError,
        ConnectionResetError
    )),
)
def extract_full_article(url: str) -> str:
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'lxml')
    for tag in soup(['script', 'style', 'iframe', 'nav', 'header', 'footer']):
        tag.decompose()

    paragraphs = soup.find_all(['p', 'article'])
    
    # Clean and join paragraphs
    cleaned_text = ' '.join(
        ' '.join(
            p.get_text()
            .strip()
            .replace('\n', ' ')
            .split()
        ) 
        for p in paragraphs 
        if p.get_text().strip()
    )
    return cleaned_text


def extract_image_url_from_description(description: str) -> str | None:
    """Extract image URL from the <img> tag inside the description HTML."""
    try:
        soup = BeautifulSoup(description, 'html.parser')
        img_tag = soup.find('img')
        if img_tag and img_tag.has_attr('src'):
            return img_tag['src']
    except Exception as e:
        print(f"Error parsing description: {e}")
    return None


# alias conversion
def slugify(name):
    return (
        name.lower()
        .replace(" ", "")
        .replace("đ", "d")
        .replace("á", "a").replace("à", "a").replace("ả", "a").replace("ã", "a").replace("ạ", "a")
        .replace("é", "e").replace("è", "e").replace("ẻ", "e").replace("ẽ", "e").replace("ẹ", "e")
        .replace("ê", "e").replace("ế", "e").replace("ề", "e").replace("ể", "e").replace("ễ", "e").replace("ệ", "e")
        .replace("í", "i").replace("ì", "i").replace("ỉ", "i").replace("ĩ", "i").replace("ị", "i")
        .replace("ó", "o").replace("ò", "o").replace("ỏ", "o").replace("õ", "o").replace("ọ", "o")
        .replace("ô", "o").replace("ố", "o").replace("ồ", "o").replace("ổ", "o").replace("ỗ", "o").replace("ộ", "o")
        .replace("ơ", "o").replace("ớ", "o").replace("ờ", "o").replace("ở", "o").replace("ỡ", "o").replace("ợ", "o")
        .replace("ú", "u").replace("ù", "u").replace("ủ", "u").replace("ũ", "u").replace("ụ", "u")
        .replace("ư", "u").replace("ứ", "u").replace("ừ", "u").replace("ử", "u").replace("ữ", "u").replace("ự", "u")
        .replace("ý", "y").replace("ỳ", "y").replace("ỷ", "y").replace("ỹ", "y").replace("ỵ", "y")
    )

def alias_from_topic(topic):
    return ''.join(word[0].lower() for word in topic.split())

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type((http.client.RemoteDisconnected, urllib.error.URLError, socket.timeout)),
)
def parse_feed_with_retry(url, logger):
    """Parse RSS feed with retry mechanism."""
    try:
        request_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        }
        return feedparser.parse(url, request_headers=request_headers)
    except Exception as e:
        logger.warning(f"Error parsing feed {url}: {e}")
        raise

