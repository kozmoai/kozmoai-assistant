import pandas as pd
from azure.storage.blob import BlobServiceClient


class ParquetWriter:
    def __init__(self, connection_string: str, container_name: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_name = container_name

    def write_parquet(self, df: pd.DataFrame, partition_path: str):
        parquet_path = f"/tmp/data.parquet"
        df.to_parquet(parquet_path, index=False)

        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=f"{partition_path}/data.parquet"
        )
        with open(parquet_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
