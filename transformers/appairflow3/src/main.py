import os
from azure_file_share_client import AzureFileShareClient
from file_processor import FileProcessor


def main():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    share_name = os.getenv("AZURE_SHARE_NAME")
    directory_name = os.getenv("AZURE_DIRECTORY_NAME")
    regex_pattern = r"^Refunds_\d{8}\.json$"

    azure_client = AzureFileShareClient(connection_string, share_name)
    file_processor = FileProcessor(azure_client)

    latest_file = file_processor.get_latest_file(directory_name, regex_pattern)
    if latest_file:
        content = file_processor.read_file(directory_name, latest_file)
        print(f"Latest file: {latest_file}")
        print(f"Content: {content}")
    else:
        print("No matching files found.")


if __name__ == "__main__":
    main()
