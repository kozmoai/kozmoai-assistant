from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.storage.fileshare import ShareFileClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import yaml
import re
from datetime import datetime


class AzureFileShareHook:
    def __init__(self, connection_string, share_name):
        self.connection_string = connection_string
        self.share_name = share_name

    def list_files(self, directory_path):
        file_client = ShareFileClient.from_connection_string(
            self.connection_string, self.share_name, directory_path
        )
        return [file.name for file in file_client.list_directories_and_files()]

    def read_file(self, file_path):
        file_client = ShareFileClient.from_connection_string(
            self.connection_string, self.share_name, file_path
        )
        return file_client.download_file().readall().decode("utf-8")


class AzureBlobStorageHook:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name

    def upload_file(self, file_path, data):
        blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=self.container_name, blob=file_path
        )
        blob_client.upload_blob(data, overwrite=True)


def load_schema_from_yaml(schema_path):
    with open(schema_path, "r") as file:
        return yaml.safe_load(file)


def apply_schema_to_dataframe(df, schema):
    for column, dtype in schema.items():
        df[column] = df[column].astype(dtype)
    return df


class RefundsFileOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        azure_file_share_conn_str,
        azure_blob_storage_conn_str,
        share_name,
        container_name,
        directory_path,
        schema_path,
        *args,
        **kwargs,
    ):
        super(RefundsFileOperator, self).__init__(*args, **kwargs)
        self.azure_file_share_conn_str = azure_file_share_conn_str
        self.azure_blob_storage_conn_str = azure_blob_storage_conn_str
        self.share_name = share_name
        self.container_name = container_name
        self.directory_path = directory_path
        self.schema_path = schema_path

    def execute(self, context):
        file_share_hook = AzureFileShareHook(
            self.azure_file_share_conn_str, self.share_name
        )
        blob_storage_hook = AzureBlobStorageHook(
            self.azure_blob_storage_conn_str, self.container_name
        )

        # List files in the directory
        files = file_share_hook.list_files(self.directory_path)

        # Filter files based on regex match
        regex = re.compile(r"^Refunds_\d{8}\.json$")
        matched_files = [f for f in files if regex.match(f)]

        if not matched_files:
            self.log.info("No matching files found.")
            return

        # Find the latest file based on date in the file name
        latest_file = max(
            matched_files,
            key=lambda f: datetime.strptime(f.split("_")[1].split(".")[0], "%Y%m%d"),
        )

        # Read the latest file
        file_content = file_share_hook.read_file(f"{self.directory_path}/{latest_file}")
        df = pd.read_json(file_content)

        # Load schema from YAML file
        schema = load_schema_from_yaml(self.schema_path)

        # Apply schema to DataFrame
        df = apply_schema_to_dataframe(df, schema)

        # Extract date from file name for partitioning
        file_date = datetime.strptime(latest_file.split("_")[1].split(".")[0], "%Y%m%d")
        partition_path = f"{file_date.year}/{file_date.month:02d}/{file_date.day:02d}"

        # Write DataFrame to Parquet in Azure Blob Storage
        parquet_data = df.to_parquet(index=False)
        blob_storage_hook.upload_file(f"{partition_path}/Refunds.parquet", parquet_data)


### dags/refunds_dag.py
