from airflow.hooks.base_hook import BaseHook
from azure.storage.fileshare import ShareServiceClient
import os


class AzureFileShareHook(BaseHook):
    def __init__(self, conn_id="azure_file_share_default"):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.service_client = ShareServiceClient(
            account_url=self.connection.host, credential=self.connection.password
        )

    def list_files(self):
        file_list = []
        share_client = self.service_client.get_share_client(self.connection.schema)
        dir_client = share_client.get_directory_client(
            self.connection.extra_dejson.get("directory")
        )
        for item in dir_client.list_directories_and_files():
            if item["name"].startswith("Refunds_") and item["name"].endswith(".json"):
                file_list.append(item["name"])
        return file_list

    def download_file(self, file_name, local_path):
        share_client = self.service_client.get_share_client(self.connection.schema)
        dir_client = share_client.get_directory_client(
            self.connection.extra_dejson.get("directory")
        )
        file_client = dir_client.get_file_client(file_name)
        with open(os.path.join(local_path, file_name), "wb") as file:
            data = file_client.download_file()
            data.readinto(file)
