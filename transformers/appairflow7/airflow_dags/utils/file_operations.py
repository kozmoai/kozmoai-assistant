import re
import os
from datetime import datetime


def list_json_files(azure_file_share_hook, directory_path, regex_pattern):
    files = azure_file_share_hook.list_files(directory_path)
    return [file["name"] for file in files if re.match(regex_pattern, file["name"])]


def copy_files_to_local(azure_file_share_hook, files, local_directory):
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    local_files = []
    for file in files:
        local_path = os.path.join(local_directory, os.path.basename(file))
        azure_file_share_hook.download_file(file, local_path)
        local_files.append(local_path)
    return local_files


def read_latest_file(files, regex_pattern):
    latest_date = None
    latest_file = None
    for file in files:
        match = re.match(regex_pattern, os.path.basename(file))
        if match:
            file_date = datetime.strptime(match.group(1), "%Y%m%d")
            if not latest_date or file_date > latest_date:
                latest_date = file_date
                latest_file = file
    return latest_file
