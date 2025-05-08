import json
from pathlib import Path
from typing import Dict
from dagster import asset

@asset
def rss_feed_list() -> Dict:
    """Load RSS feed list from JSON config file."""
    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    file_path = base_dir / "config" / "rss_feeds.json"
    
    with open(file_path, "r", encoding="utf-8") as f:
        rss_sources = json.load(f)
    return rss_sources