from airflow.hooks.base_hook import BaseHook
from azure.storage.fileshare import ShareFileClient


class AzureFileShareHook(BaseHook):
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.share_name = self.connection.extra_dejson.get("share_name")
        self.file_client = ShareFileClient.from_connection_string(
            self.connection.extra_dejson.get("connection_string"),
            share_name=self.share_name,
        )

    def list_files(self, directory_path):
        return self.file_client.list_directories_and_files(directory_path)

    def download_file(self, file_path, local_path):
        with open(local_path, "wb") as file_handle:
            data = self.file_client.download_file(file_path)
            file_handle.write(data.readall())
