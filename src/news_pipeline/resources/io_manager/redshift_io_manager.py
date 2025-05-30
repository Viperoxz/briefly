from dagster import IOManager, io_manager, OutputContext, InputContext
import pandas as pd
import sqlalchemy as sa
import os
from sqlalchemy.exc import SQLAlchemyError
from dagster import get_dagster_logger

class RedshiftIOManager(IOManager):
    """IO Manager for reading and writing data to Amazon Redshift"""
    
    def __init__(self, config):
        self.user = config["user"] 
        self.password = config["password"]
        self.host = config["host"]
        self.port = config["port"]
        self.database = config["database"]
        self.schema = config.get("schema", "public")
        self._engine = None
        self.logger = get_dagster_logger()
        
    def get_engine(self):
        """Get or create SQLAlchemy engine for Redshift"""
        if self._engine is None:
            conn_str = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self._engine = sa.create_engine(conn_str)
        return self._engine
        
    def _get_table_name(self, context):
        return context.asset_key.path[-1]
    
    def handle_output(self, context: OutputContext, obj):
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            self.logger.warning("Empty data received, skipping Redshift write")
            return
            
        engine = self.get_engine()
        table_name = self._get_table_name(context)
        
        if not isinstance(obj, pd.DataFrame):
            if isinstance(obj, dict):
                obj = pd.DataFrame([obj])
            else:
                self.logger.error(f"Unsupported data type for Redshift IO Manager: {type(obj)}")
                return
        
        try:
            # Use a transaction to ensure atomicity
            with engine.begin() as conn:
                obj.to_sql(
                    name=table_name,
                    con=conn,
                    schema=self.schema,
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                
            self.logger.info(f"Successfully wrote {len(obj)} rows to {self.schema}.{table_name}")
        except SQLAlchemyError as e:
            self.logger.error(f"Error writing to Redshift: {str(e)}")
            raise
            
    def load_input(self, context: InputContext):
        engine = self.get_engine()
        table_name = self._get_table_name(context)
        
        try:
            query = f"SELECT * FROM {self.schema}.{table_name}"
            
            if context.has_partition_key:
                # Assuming the table has a column that can be used for partitioning
                # You may need to adjust this based on your actual partitioning scheme
                partition_column = "url"  # Change this if needed
                query += f" WHERE {partition_column} = '{context.partition_key}'"
                
            return pd.read_sql(query, engine)
        except SQLAlchemyError as e:
            self.logger.error(f"Error reading from Redshift: {str(e)}")
            return pd.DataFrame()
            
@io_manager(config_schema={
    "user": str,
    "password": str,
    "host": str, 
    "port": int,
    "database": str,
    "schema": str
})
def redshift_io_manager(init_context):
    return RedshiftIOManager(config=init_context.resource_config)