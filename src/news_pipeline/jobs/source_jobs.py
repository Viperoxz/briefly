from dagster import define_asset_job, AssetSelection


sources_topics_job = define_asset_job(
    name="sources_topics_job",
    selection=AssetSelection.keys("rss_feed_list", "sources", "topics")
)