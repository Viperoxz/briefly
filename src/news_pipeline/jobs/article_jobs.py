from dagster import (
    define_asset_job,
    AssetSelection,
    RetryPolicy,
    DynamicPartitionsDefinition
)

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

articles_update_job = define_asset_job(
    name="articles_update_job",
    selection=AssetSelection.keys("rss_feed_list", "articles"),
    config={"ops": {"articles": {"config": {"save_json": False}}}}
)

articles_processing_job = define_asset_job(
    name="articles_processing_job",
    selection=AssetSelection.keys("articles_with_summary"),
    partitions_def=article_partitions_def,
    op_retry_policy=RetryPolicy(max_retries=3)
)

articles_embedding_job = define_asset_job(
    name="articles_embedding_job",
    selection=AssetSelection.keys("embedded_articles"),
    partitions_def=article_partitions_def,
    op_retry_policy=RetryPolicy(max_retries=3)
)


related_articles_job = define_asset_job(
    name="related_articles_job",
    selection=AssetSelection.keys("related_articles"),
    partitions_def=article_partitions_def,
    op_retry_policy=RetryPolicy(max_retries=3)
)