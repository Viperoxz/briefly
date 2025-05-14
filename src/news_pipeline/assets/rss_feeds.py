import json
from pathlib import Path
from typing import Dict, Any
from dagster import asset, Output

@asset(io_manager_key="mongo_io_manager",
       kinds={"json"},
       description="Load RSS feed list from JSON config file.")
def rss_feed_list() -> Output[Dict[str, Any]]:
    base_dir = Path(__file__).resolve().parent.parent.parent.parent
    file_path = base_dir / "config" / "rss_feeds.json"
    
    with open(file_path, "r", encoding="utf-8") as f:
        rss_sources = json.load(f)
    return Output(value=rss_sources, metadata={"source_count": len(rss_sources)})