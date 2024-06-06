import re
from datetime import datetime
from azure_file_share_client import AzureFileShareClient


class FileProcessor:

    def __init__(self, azure_client: AzureFileShareClient):
        self.azure_client = azure_client

    def get_latest_file(self, directory_name: str, regex_pattern: str) -> str:
        files = self.azure_client.list_files(directory_name)
        matched_files = [f for f in files if re.match(regex_pattern, f)]
        
        if not matched_files:
            return None
        
        # Extract date from file name and sort by date
        def extract_date(file_name: str) -> datetime:
            match = re.search(r'\d{8}', file_name)
            if match:
                return datetime.strptime(match.group(), '%Y%m%d')
            return datetime.min
        
        matched_files.sort(key=extract_date, reverse=True)
        return matched_files[0]

    def read_file(self, directory_name: str, file_name: str) -> str:
        file_client = self.azure_client.share_client.get_file_client(f"{directory_name}/{file_name}")
        download = file_client.download_file()
        return download.readall().decode('utf-8')


I need an airflow task as python operator that lists all json files in a specific path in azure files share and then reads the latest file based on regex match and a date in the file name, for example I have 4 files under the same path : Refunds_20240606.json / Refunds_20240605.json / Ref_20240607.json / Refunds_rejections_20240606.json, the code should read only one file Refunds_20240606.json
