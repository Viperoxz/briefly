from dagster import asset, AssetIn, Output, get_dagster_logger
import pandas as pd
import datetime
import sqlalchemy as sa

@asset(
    description="Load processed article data into Redshift data warehouse",
    key="warehouse_articles",
    io_manager_key="redshift_io_manager",
    ins={"processed_articles": AssetIn(key="processed_articles")},
    group_name="warehouse",
    compute_kind="redshift",
    required_resource_keys={"redshift_io_manager"}
)
def warehouse_articles(context, processed_articles) -> Output[pd.DataFrame]:
    """Load the processed articles into the Redshift data warehouse."""
    logger = get_dagster_logger()
    
    if processed_articles.empty:
        logger.info("No processed articles to load into warehouse")
        return Output(
            value=pd.DataFrame(), 
            metadata={"num_rows": 0, "status": "empty_input"}
        )
    
    # Add warehouse-specific fields or transformations if needed
    processed_articles["load_timestamp"] = datetime.datetime.now()
    
    # Get Redshift connection
    redshift_engine = context.resources.redshift_io_manager.get_engine()
    
    # Ensure table exists with correct schema
    try:
        with redshift_engine.begin() as conn:
            # Check if the table exists, if not create it
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS news.articles (
                    article_id VARCHAR(36) PRIMARY KEY,
                    url VARCHAR(512) NOT NULL,
                    title VARCHAR(256) NOT NULL,
                    source_id VARCHAR(36) NOT NULL,
                    topic_id VARCHAR(36) NOT NULL,
                    image VARCHAR(512),
                    published_date TIMESTAMP,
                    content_length INTEGER,
                    content_clean TEXT,
                    alias VARCHAR(256),
                    crawl_timestamp TIMESTAMP,
                    process_timestamp TIMESTAMP,
                    load_timestamp TIMESTAMP,
                    language VARCHAR(10),
                    content_hash VARCHAR(64),
                    UNIQUE(url)
                );
            """))
    except Exception as e:
        logger.error(f"Error ensuring table exists: {str(e)}")
        raise
    
    # Get the number of rows before insertion
    try:
        with redshift_engine.connect() as conn:
            count_before = conn.execute(sa.text("SELECT COUNT(*) FROM news.articles")).scalar()
    except Exception as e:
        logger.error(f"Error getting row count: {str(e)}")
        count_before = 0
    
    # Count potential new rows and duplicates
    try:
        with redshift_engine.begin() as conn:
            # Insert with ON CONFLICT DO NOTHING to handle duplicates
            # Convert DataFrame to records and use executemany for better performance
            inserted = 0
            for _, row in processed_articles.iterrows():
                try:
                    # Handle datetime objects for SQL insertion
                    for col in row.index:
                        if isinstance(row[col], pd.Timestamp):
                            row[col] = row[col].to_pydatetime()
                    
                    # Create SQL with parameters
                    param_str = ", ".join([f":{col}" for col in row.index])
                    cols_str = ", ".join([col for col in row.index])
                    
                    # Use parameterized query to avoid SQL injection
                    query = sa.text(f"""
                        INSERT INTO news.articles ({cols_str})
                        VALUES ({param_str})
                        ON CONFLICT (url) DO NOTHING
                    """)
                    
                    result = conn.execute(query, row.to_dict())
                    inserted += result.rowcount
                except Exception as e:
                    logger.error(f"Error inserting row: {str(e)}")
    except Exception as e:
        logger.error(f"Error during batch insert: {str(e)}")
        raise
    
    # Get the number of rows after insertion
    try:
        with redshift_engine.connect() as conn:
            count_after = conn.execute(sa.text("SELECT COUNT(*) FROM news.articles")).scalar()
    except Exception as e:
        logger.error(f"Error getting final row count: {str(e)}")
        count_after = 0
    
    actual_inserted = count_after - count_before
    
    logger.info(f"Loaded {actual_inserted} new articles into Redshift warehouse")
    
    # Return the DataFrame for downstream assets if needed
    return Output(
        value=processed_articles,
        metadata={
            "num_rows": len(processed_articles),
            "new_rows": actual_inserted,
            "duplicates": len(processed_articles) - actual_inserted,
            "table": "articles",
            "schema": "news",
            "load_timestamp": datetime.datetime.now().isoformat()
        }
    )