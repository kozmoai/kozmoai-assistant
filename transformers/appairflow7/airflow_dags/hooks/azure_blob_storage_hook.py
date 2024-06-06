from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient


class AzureBlobStorageHook(BaseHook):
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.container_name = self.connection.extra_dejson.get("container_name")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection.extra_dejson.get("connection_string")
        )

    def upload_file(self, local_path, blob_path):
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=blob_path
        )
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data)
