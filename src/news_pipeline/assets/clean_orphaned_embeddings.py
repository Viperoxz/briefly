from dagster import asset, Output, get_dagster_logger, AssetIn
import pandas as pd

@asset(
    io_manager_key="qdrant_io_manager",
    ins={"synced_articles": AssetIn(key="synced_articles")}
)
def clean_orphaned_embeddings(context, synced_articles: pd.DataFrame):
    logger = get_dagster_logger()

    mongo_links = set(synced_articles["url"])
    qdrant_points = context.resources.qdrant_io_manager.load_input(context)
    qdrant_ids = {point.id for point in qdrant_points}

    orphaned_ids = qdrant_ids - mongo_links
    if orphaned_ids:
        context.resources.qdrant_io_manager.client.delete(
            collection_name=context.resources.qdrant_io_manager.collection_name,
            points_selector=list(orphaned_ids)
        )
        logger.info(f"Deleted {len(orphaned_ids)} orphaned embeddings")

    return Output(
        value=None,
        metadata={"num_deleted": len(orphaned_ids)}
    )