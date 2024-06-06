import re
from datetime import datetime
from plugins.hooks.azure_file_share_hook import AzureFileShareHook

def list_json_files(path):
    connection_string = 'your_connection_string'
    share_name = 'your_share_name'
    directory_name = path
    hook = AzureFileShareHook(connection_string)
    files = hook.list_files(share_name, directory_name)
    return [file for file in files if file.endswith('.json')]

def get_latest_file(files):
    regex = re.compile(r'Refunds_\d{8}\.json')
    matched_files = [file for file in files if regex.match(file)]
    if not matched_files:
        return None
    matched_files.sort(key=lambda x: datetime.strptime(x.split('_')[1].split('.')[0], '%Y%m%d'), reverse=True)
    return matched_files[0]

def read_file(file_name):
    connection_string = 'your_connection_string'
    share_name = 'your_share_name'
    directory_name = 'your/azure/file/share/path'
    hook = AzureFileShareHook(connection_string)
    return hook.read_file(share_name, directory_name, file_name)



I need a python code that lists all json files in a specific path in azure files share based on a regex match if the file name starts with `Refunds_`, then reads the latest file based on regex match `Refunds_` and a date in the file name, for example I have 4 files under the same path : Refunds_20240606.json / Refunds_20240605.json / Ref_20240607.json / Refunds_rejections_20240606.json, the code should read only one file Refunds_20240606.json, then applies a specific schema to the loaded dataframe base on an input schema from yaml file  then writes the dataframe into a parquet in azure blob storage based an a partition YYYY/MM/DD created from the date in the file name
