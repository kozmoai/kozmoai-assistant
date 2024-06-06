from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io


class AzureBlobStorageHook(BaseHook):
    def __init__(self, conn_id="azure_blob_storage_default"):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.service_client = BlobServiceClient.from_connection_string(
            self.connection.extra_dejson["connection_string"]
        )
        self.container_name = self.connection.extra_dejson["container_name"]

    def write_parquet(self, df, partition_path):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_client = self.service_client.get_blob_client(
            container=self.container_name, blob=f"{partition_path}/data.parquet"
        )
        blob_client.upload_blob(buffer, overwrite=True)
