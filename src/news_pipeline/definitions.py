from dagster import (
    Definitions,
    DynamicPartitionsDefinition,
    multiprocess_executor
)
from .assets import (
    rss_feed_list,
    sources,
    topics,
    raw_articles,
    articles_with_summary,
    embedded_articles,
    text_to_speech,
    related_articles
)
from .resources.io_manager.mongo_io_manager import MongoDBIOManager
from .resources.io_manager.qdrant_io_manager import QdrantIOManager
from .resources.io_manager.s3_io_manager import s3_io_manager

import os
from dotenv import load_dotenv
from .sensors import (article_partition_sensor, 
                      embedding_partition_sensor, 
                      tts_partition_sensor, 
                      related_articles_sensor)
from .jobs import (sources_topics_job, 
                   articles_update_job, 
                   articles_processing_job, 
                   articles_embedding_job, 
                   articles_tts_job,
                   related_articles_job)
from .schedules import sources_topics_schedule, articles_update_schedule


load_dotenv()

article_partitions_def = DynamicPartitionsDefinition(name="article_partitions")

MONGO_CONFIG = {
    "uri": os.getenv("MONGO_URI"),
    "database": os.getenv("MONGO_DB")
}

QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL"),
    "api_key": os.getenv("QDRANT_API_KEY")
}

MAX_CONCURRENCIES = int(os.getenv("MAX_CONCURRENCIES", 4))

article_partitions_sensor = article_partition_sensor

defs = Definitions(
    assets=[
        rss_feed_list,
        sources,
        topics,
        raw_articles,
        articles_with_summary,
        text_to_speech,
        embedded_articles,
        related_articles
    ],
    resources={
        "s3_io_manager": s3_io_manager.configured({
            "bucket": {"env": "S3_BUCKET_NAME"},
            "region": {"env": "AWS_REGION"}
        }),
        "mongo_io_manager": MongoDBIOManager(MONGO_CONFIG),
        "qdrant_io_manager": QdrantIOManager(QDRANT_CONFIG)
    },
    jobs=[sources_topics_job, 
          articles_update_job, 
          articles_processing_job, 
          articles_embedding_job, 
          articles_tts_job, 
          related_articles_job],
    schedules=[sources_topics_schedule, articles_update_schedule],
    sensors=[article_partition_sensor, 
             embedding_partition_sensor, 
             tts_partition_sensor,
             related_articles_sensor],
    executor=multiprocess_executor.configured(
        {"max_concurrent": MAX_CONCURRENCIES}
    )
)