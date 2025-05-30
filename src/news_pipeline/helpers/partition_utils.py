import re
import hashlib

def clean_partition_key(key):
    """
    Clean a partition key to be safe for use in file paths
    """
    if not key:
        return "unknown"
    
    # If the key is a URL, hash it to make it shorter and safer for filesystems
    if key.startswith("http"):
        return hashlib.md5(key.encode()).hexdigest()
    
    # Otherwise clean it up for safe use as a filename
    return re.sub(r'[^\w\-\.]', '_', key)