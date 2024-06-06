from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient


class AzureBlobStorageHook(BaseHook):
    def __init__(self, conn_id="azure_blob_storage_default"):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.service_client = BlobServiceClient(
            account_url=self.connection.host, credential=self.connection.password
        )

    def upload_file(self, local_file, container_name, blob_name):
        blob_client = self.service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
