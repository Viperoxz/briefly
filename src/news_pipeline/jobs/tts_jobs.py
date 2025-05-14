from dagster import define_asset_job, AssetSelection, RetryPolicy
from dagster._core.definitions import DynamicPartitionsDefinition

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

articles_tts_job = define_asset_job(
    name="articles_tts_job",
    selection=AssetSelection.keys("text_to_speech"),
    partitions_def=article_partitions_def,
    op_retry_policy=RetryPolicy(max_retries=3)
)