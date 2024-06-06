from airflow.hooks.base_hook import BaseHook
from azure.storage.fileshare import ShareServiceClient
import os


class AzureFileShareHook(BaseHook):
    def __init__(self, conn_id="azure_file_share_default"):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.service_client = ShareServiceClient.from_connection_string(
            self.connection.extra_dejson["connection_string"]
        )
        self.share_name = self.connection.extra_dejson["share_name"]
        self.directory_name = self.connection.extra_dejson["directory_name"]

    def list_files(self):
        file_list = []
        file_client = self.service_client.get_directory_client(
            self.share_name, self.directory_name
        )
        for item in file_client.list_directories_and_files():
            if item["is_directory"] == False:
                file_list.append(item["name"])
        return file_list

    def read_file(self, file_name):
        file_client = self.service_client.get_file_client(
            self.share_name, os.path.join(self.directory_name, file_name)
        )
        download = file_client.download_file()
        return download.readall().decode("utf-8")
