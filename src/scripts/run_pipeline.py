from dagster import (
    load_assets_from_modules,
    materialize,
    DynamicPartitionsDefinition,
    RunConfig,
    AssetSelection,
    DagsterInstance,
    multiprocess_executor
)
from src.news_pipeline import assets
from src.news_pipeline.resources.mongo_io_manager import MongoDBIOManager
from src.news_pipeline.resources.qdrant_io_manager import QdrantIOManager
import os
from dotenv import load_dotenv
import time

load_dotenv()

def initialize_partitions(instance: DagsterInstance):
    """Initialize dynamic partitions based on MongoDB articles."""
    mongo_config = {
        "uri": os.getenv("MONGO_URI"),
        "database": os.getenv("MONGO_DB")
    }
    mongo_io_manager = MongoDBIOManager(mongo_config)
    
    # Fetch all article links from MongoDB
    collection = mongo_io_manager._get_collection(None)
    articles = collection.find({}, {"link": 1})
    links = [article["link"] for article in articles]
    
    # Get the partitions definition
    partitions_def = DynamicPartitionsDefinition(name="article_partitions")
    
    # Get existing partitions
    existing_partitions = instance.get_dynamic_partitions("article_partitions")
    
    # Add new partitions and remove obsolete ones
    new_partitions = set(links)
    partitions_to_add = new_partitions - set(existing_partitions)
    partitions_to_remove = set(existing_partitions) - new_partitions
    
    for partition in partitions_to_add:
        instance.add_dynamic_partitions("article_partitions", [partition])
        print(f"Added partition: {partition}")
    
    for partition in partitions_to_remove:
        instance.delete_dynamic_partition("article_partitions", partition)
        print(f"Removed partition: {partition}")

def main():
    # Initialize Dagster instance
    instance = DagsterInstance.get()
    
    # Load assets
    all_assets = load_assets_from_modules([assets])

    while True:
        # Materialize raw_articles to ensure we have the latest articles
        raw_articles_result = materialize(
            assets=all_assets,
            selection=AssetSelection.keys("articles"),
            run_config=RunConfig(),
            resources={
                "mongo_io_manager": MongoDBIOManager({
                    "uri": os.getenv("MONGO_URI"),
                    "database": os.getenv("MONGO_DB")
                }),
                "qdrant_io_manager": QdrantIOManager({
                    "url": os.getenv("QDRANT_URL"),
                    "api_key": os.getenv("QDRANT_API_KEY")
                }),
            },
            instance=instance
        )

        if not raw_articles_result.success:
            print("Failed to materialize raw_articles!")
        else:
            print("Successfully materialized raw_articles!")
            initialize_partitions(instance)

            # Materialize all partitions
            partitions = instance.get_dynamic_partitions("article_partitions")
            if partitions:
                for partition in partitions:
                    result = materialize(
                        assets=all_assets,
                        selection=AssetSelection.keys("summarized_articles", "embedded_articles", "synced_articles", "qdrant_embeddings"),
                        run_config=RunConfig(
                            ops={
                                "summarized_articles": {},
                                "embedded_articles": {},
                                "synced_articles": {}
                            }
                        ),
                        partition_key=partition,
                        resources={
                            "mongo_io_manager": MongoDBIOManager({
                                "uri": os.getenv("MONGO_URI"),
                                "database": os.getenv("MONGO_DB")
                            }),
                            "qdrant_io_manager": QdrantIOManager({
                                "url": os.getenv("QDRANT_URL"),
                                "api_key": os.getenv("QDRANT_API_KEY")
                            }),
                        },
                        instance=instance
                    )

                    if result.success:
                        print(f"Successfully materialized partition: {partition}")
                    else:
                        print(f"Failed to materialize partition: {partition}")
            else:
                print("No partitions to materialize!")

        # Chờ 5 phút trước khi chạy lại
        time.sleep(300)  # 300 giây = 5 phút

if __name__ == "__main__":
    main()