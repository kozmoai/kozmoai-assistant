from azure.storage.fileshare import ShareFileClient, ShareDirectoryClient
import re
from typing import List


class AzureFileManager:
    def __init__(self, connection_string: str, file_share_name: str, directory: str):
        self.connection_string = connection_string
        self.file_share_name = file_share_name
        self.directory = directory

    def list_files(self) -> List[str]:
        directory_client = ShareDirectoryClient.from_connection_string(
            self.connection_string, self.file_share_name, self.directory
        )
        files = []
        for item in directory_client.list_directories_and_files():
            if item["is_directory"]:
                continue
            files.append(item["name"])
        return files

    def download_file(self, file_name: str) -> str:
        file_client = ShareFileClient.from_connection_string(
            self.connection_string,
            self.file_share_name,
            f"{self.directory}/{file_name}",
        )
        download_path = f"/tmp/{file_name}"
        with open(download_path, "wb") as file:
            data = file_client.download_file()
            data.readinto(file)
        return download_path
