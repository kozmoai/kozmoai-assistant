import os
import re
import pandas as pd
from azure_file_share_hook import AzureFileShareHook
from datetime import datetime
from pyarrow import parquet as pq


def list_json_files(azure_conn_id, share_name, directory_name):
    hook = AzureFileShareHook(azure_conn_id)
    files = hook.list_files(share_name, directory_name)
    json_files = [file for file in files if re.match(r"^Refunds_.*\.json$", file)]
    return json_files


def copy_files_to_local(azure_conn_id, share_name, directory_name, local_path):
    hook = AzureFileShareHook(azure_conn_id)
    json_files = list_json_files(azure_conn_id, share_name, directory_name)
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    for file in json_files:
        hook.download_file(share_name, directory_name, file, local_path)


def read_latest_file(local_path):
    files = [f for f in os.listdir(local_path) if re.match(r"^Refunds_\d{8}\.json$", f)]
    latest_file = max(
        files, key=lambda x: datetime.strptime(re.search(r"\d{8}", x).group(), "%Y%m%d")
    )
    df = pd.read_json(f"{local_path}/{latest_file}")
    return df, latest_file


def apply_schema(df, schema):
    for col, dtype in schema.items():
        if dtype.lower() == "string":
            df[col] = df[col].astype(str)
        elif dtype.lower().startswith("varchar"):
            df[col] = df[col].astype(str)
        elif dtype.lower().startswith("float"):
            df[col] = df[col].astype(float)
    return df


def write_to_parquet(df, local_path, latest_file):
    date_str = re.search(r"\d{8}", latest_file).group()
    date = datetime.strptime(date_str, "%Y%m%d")
    partition_path = f"{local_path}/parquet/{date.year}/{date.month:02d}/{date.day:02d}"
    if not os.path.exists(partition_path):
        os.makedirs(partition_path)
    parquet_file = f"{partition_path}/Refunds_{date_str}.parquet"
    df.to_parquet(parquet_file)
    return parquet_file


def upload_to_blob_storage(local_path, azure_conn_id):
    from azure.storage.blob import BlobServiceClient

    hook = AzureFileShareHook(azure_conn_id)
    conn = hook.get_conn()
    blob_service_client = BlobServiceClient(
        account_url=conn.account_url, credential=conn.credential
    )
    container_name = "your-container-name"
    parquet_files = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(local_path)
        for f in filenames
        if f.endswith(".parquet")
    ]
    for file in parquet_files:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=file.replace(local_path, "").lstrip("/")
        )
        with open(file, "rb") as data:
            blob_client.upload_blob(data)
