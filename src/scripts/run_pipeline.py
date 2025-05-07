from dagster import (
    load_assets_from_modules,
    materialize,
    DynamicPartitionsDefinition,
    RunConfig,
    AssetSelection,
    DagsterInstance,
)
from src.news_pipeline import assets
from src.news_pipeline.resources.mongo_io_manager import MongoDBIOManager
from src.news_pipeline.resources.qdrant_io_manager import QdrantIOManager
import os
from dotenv import load_dotenv

load_dotenv()

def initialize_partitions(instance: DagsterInstance):
    """Initialize dynamic partitions based on MongoDB articles."""
    mongo_config = {
        "uri": os.getenv("MONGO_URI"),
        "database": os.getenv("MONGO_DB")
    }
    mongo_io_manager = MongoDBIOManager(mongo_config)
    
    # Fetch all article links from MongoDB
    collection = mongo_io_manager._get_collection(None)  # Context is None for manual query
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
    
    # Initialize partitions
    initialize_partitions(instance)

    # Load assets
    all_assets = load_assets_from_modules([assets])

    # Define run config for partitioned execution
    run_config = RunConfig(
        ops={"synchronized_articles": {}}  # No specific op config needed
    )

    # Execute pipeline
    result = materialize(
        assets=all_assets,
        selection=AssetSelection.all(),  # Select all assets
        run_config=run_config,
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
        print("Pipeline executed successfully!")
    else:
        print("Pipeline failed!")

if __name__ == "__main__":
    main()