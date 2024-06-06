from typing import List

from azure.storage.fileshare import ShareServiceClient


class AzureFileShareClient:
    def __init__(self, connection_string: str, share_name: str):
        self.service_client = ShareServiceClient.from_connection_string(
            conn_str=connection_string
        )
        self.share_client = self.service_client.get_share_client(share_name)

    def list_files(self, directory_name: str) -> List[str]:
        directory_client = self.share_client.get_directory_client(directory_name)
        file_list = []
        for item in directory_client.list_directories_and_files():
            if not item["is_directory"]:
                file_list.append(item["name"])
        return file_list
