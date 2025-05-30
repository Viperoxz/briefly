import boto3
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from dagster import IOManager, io_manager, OutputContext, InputContext
from ...helpers import clean_partition_key

class S3ParquetIOManager(IOManager):
    """IO Manager for storing and retrieving data in parquet format on S3"""
    
    def __init__(self, config):
        self.bucket = config["bucket"]
        self.prefix = config.get("prefix", "")
        self.region = config["region"]
        self.s3_client = boto3.client(
            's3', 
            region_name=self.region,
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
    
    def _get_path(self, context):
        if context.has_partition_key:
            partition_str = clean_partition_key(context.partition_key)
            return f"{self.prefix}/{context.asset_key.path[-1]}/{partition_str}.parquet"
        return f"{self.prefix}/{context.asset_key.path[-1]}.parquet"
    
    def handle_output(self, context: OutputContext, obj):
        path = self._get_path(context)
        
        if not isinstance(obj, pd.DataFrame):
            obj = pd.DataFrame([obj])
        
        buffer = BytesIO()
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=path,
            Body=buffer.getvalue()
        )
        
        context.log.info(f"Wrote parquet data to s3://{self.bucket}/{path}")
    
    def load_input(self, context: InputContext):
        path = self._get_path(context)
        
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=path)
            buffer = BytesIO(obj["Body"].read())
            table = pq.read_table(buffer)
            return table.to_pandas()
        except Exception as e:
            context.log.error(f"Failed to load from s3://{self.bucket}/{path}: {e}")
            return pd.DataFrame()

@io_manager(config_schema={"bucket": str, "region": str, "prefix": str})
def s3_parquet_io_manager(init_context):
    return S3ParquetIOManager(config=init_context.resource_config)