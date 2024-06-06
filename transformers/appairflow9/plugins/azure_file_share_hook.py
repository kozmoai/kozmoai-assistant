from airflow.hooks.base_hook import BaseHook
from azure.storage.fileshare import ShareServiceClient


class AzureFileShareHook(BaseHook):
    def __init__(self, azure_conn_id="azure_default"):
        self.conn_id = azure_conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        return ShareServiceClient(
            account_url=conn.extra_dejson.get("account_url"), credential=conn.password
        )

    def list_files(self, share_name, directory_name):
        share_client = self.connection.get_share_client(share_name)
        directory_client = share_client.get_directory_client(directory_name)
        return [file.name for file in directory_client.list_directories_and_files()]

    def download_file(self, share_name, directory_name, file_name, local_path):
        share_client = self.connection.get_share_client(share_name)
        directory_client = share_client.get_directory_client(directory_name)
        file_client = directory_client.get_file_client(file_name)
        with open(f"{local_path}/{file_name}", "wb") as file_handle:
            data = file_client.download_file()
            data.readinto(file_handle)
