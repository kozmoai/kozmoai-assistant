import re
from datetime import datetime

import pandas as pd

from azure_file_processor.azure_file_manager import AzureFileManager
from azure_file_processor.schema_loader import SchemaLoader
from azure_file_processor.parquet_writer import ParquetWriter


class FileProcessor:
    def __init__(
        self,
        azure_file_manager: AzureFileManager,
        schema_loader: SchemaLoader,
        parquet_writer: ParquetWriter,
    ):
        self.azure_file_manager = azure_file_manager
        self.schema_loader = schema_loader
        self.parquet_writer = parquet_writer

    def process_files(self):
        files = self.azure_file_manager.list_files()
        regex = re.compile(r"Refunds_(\d{8})\.json")
        matched_files = [f for f in files if regex.match(f)]

        if not matched_files:
            print("No matching files found.")
            return

        latest_file = max(
            matched_files,
            key=lambda x: datetime.strptime(regex.match(x).group(1), "%Y%m%d"),
        )
        file_path = self.azure_file_manager.download_file(latest_file)

        df = pd.read_json(file_path)
        schema = self.schema_loader.load_schema()
        df = df.astype(schema)

        date_str = regex.match(latest_file).group(1)
        date = datetime.strptime(date_str, "%Y%m%d")
        partition_path = f"{date.year}/{date.month:02d}/{date.day:02d}"

        self.parquet_writer.write_parquet(df, partition_path)
