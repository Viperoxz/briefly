import boto3
import os
import json
import pickle
from io import BytesIO
from dagster import IOManager, io_manager, OutputContext, InputContext
from dagster import build_output_context
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class RawS3IOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self.bucket_name = config["bucket"]
        self.region = config.get("region", os.getenv("AWS_REGION", "ap-southeast-2"))
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
        )

    def _get_s3_key(self, context):
        s3_key = context.metadata.get("s3_key")
        if s3_key:
            return s3_key
        return f"raw_data/{context.name}_{context.run_id}.json" # Fallback: Default key format

    def handle_output(self, context: OutputContext, obj):
        """Write output data to S3 (Raw layer: json)"""
        key = self._get_s3_key(context)
        if isinstance(obj, pd.DataFrame):
            body = obj.to_json(orient="records", force_ascii=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body,
                ContentType='application/json'
            )
            context.log.info(f"Stored raw JSON at s3://{self.bucket_name}/{key}")
        elif isinstance(obj, (dict, list)):
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(obj, ensure_ascii=False),
                ContentType='application/json'
            )
            context.log.info(f"Stored raw JSON at s3://{self.bucket_name}/{key}")
        else:
            # Fallback to pickle for other data types
            binary_data = pickle.dumps(obj)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key.replace('.json', '.pkl'),
                Body=binary_data
            )
            context.log.info(f"Stored raw pickle at s3://{self.bucket_name}/{key.replace('.json', '.pkl')}")

    def load_input(self, context: InputContext):
        """Load input data from S3"""
        key = self._get_s3_key(context.upstream_output)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            body = response['Body'].read().decode('utf-8')
            try:
                return pd.read_json(body)
            except Exception:
                return json.loads(body)
        except Exception as e:
            raise RuntimeError(f"Failed to load data from S3: {e}")


@io_manager(config_schema={"bucket": str, "region": str})
def s3_io_manager(init_context):
    return RawS3IOManager(config=init_context.resource_config)


def test_upload_to_s3():
    # Giả lập context để test
    context = build_output_context(
        name="test_output",
        step_key="test_step",
        run_id="test_run_id_123",
        definition_metadata={"source": "test_source"}  
    )

    # Tạo instance IOManager
    io_manager = RawS3IOManager(config={
        "bucket": os.environ.get("S3_BUCKET_NAME"),
        "region": os.environ.get("AWS_REGION", "ap-southeast-2")
    })

    # Dữ liệu test
    data = {"message": "Xin chào"}

    # Gọi hàm handle_output để upload
    io_manager.handle_output(context, data)
    print("✅ Uploaded successfully.")

if __name__ == "__main__":
    # Chạy test upload
    test_upload_to_s3()