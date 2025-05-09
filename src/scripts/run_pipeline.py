# from dagster import (
#     load_assets_from_modules,
#     materialize,
#     RunConfig,
#     AssetSelection,
#     DagsterInstance,
#     multiprocess_executor
# )
# from src.news_pipeline import assets
# from src.news_pipeline.resources.mongo_io_manager import MongoDBIOManager
# from src.news_pipeline.resources.qdrant_io_manager import QdrantIOManager
# import os
# from dotenv import load_dotenv
# import time

# load_dotenv()

# def main():
#     instance = DagsterInstance.get()
#     all_assets = load_assets_from_modules([assets])

#     while True:
#         raw_articles_result = materialize(
#             assets=all_assets,
#             selection=AssetSelection.keys("articles"),
#             run_config=RunConfig(),
#             resources={
#                 "mongo_io_manager": MongoDBIOManager({
#                     "uri": os.getenv("MONGO_URI"),
#                     "database": os.getenv("MONGO_DB")
#                 }),
#                 "qdrant_io_manager": QdrantIOManager({
#                     "url": os.getenv("QDRANT_URL"),
#                     "api_key": os.getenv("QDRANT_API_KEY")
#                 }),
#             },
#             instance=instance
#         )

#         if not raw_articles_result.success:
#             print("Failed to materialize raw_articles")
#         else:
#             print("Successfully materialized raw_articles")

#         time.sleep(300)

# if __name__ == "__main__":
#     main()