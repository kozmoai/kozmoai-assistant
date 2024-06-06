from azure.storage.fileshare import ShareServiceClient
import os


class AzureFileShareHook:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.service_client = ShareServiceClient.from_connection_string(
            conn_str=connection_string
        )

    def list_files(self, share_name, directory_name):
        share_client = self.service_client.get_share_client(share_name)
        directory_client = share_client.get_directory_client(directory_name)
        return [file.name for file in directory_client.list_directories_and_files()]

    def read_file(self, share_name, directory_name, file_name):
        share_client = self.service_client.get_share_client(share_name)
        file_client = share_client.get_file_client(
            os.path.join(directory_name, file_name)
        )
        download = file_client.download_file()
        return download.readall().decode("utf-8")
