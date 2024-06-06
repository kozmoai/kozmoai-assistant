from azure_file_processor.azure_file_manager import AzureFileManager
from azure_file_processor.file_processor import FileProcessor
from azure_file_processor.schema_loader import SchemaLoader
from azure_file_processor.parquet_writer import ParquetWriter


def main():
    # Configuration
    file_share_name = "your-file-share-name"
    file_share_directory = "your-directory"
    connection_string = "your-azure-storage-connection-string"
    schema_file_path = "path/to/your/schema.yaml"
    blob_container_name = "your-blob-container"
    blob_connection_string = "your-blob-connection-string"

    # Initialize components
    azure_file_manager = AzureFileManager(connection_string, file_share_name, file_share_directory)
    schema_loader = SchemaLoader(schema_file_path)
    parquet_writer = ParquetWriter(blob_connection_string, blob_container_name)

    # Process files
    file_processor = FileProcessor(azure_file_manager, schema_loader, parquet_writer)
    file_processor.process_files()


if __name__ == "__main__":
    main()



I need an airflow tasks that lists all json files in a specific path in azure files share based on a regex match if the file name starts with `Refunds_`, then copies into local airflow and reads the latest file based on regex match `Refunds_` and a date in the file name, for example I have 4 files under the same path : Refunds_20240606.json / Refunds_20240605.json / Ref_20240607.json / Refunds_rejections_20240606.json, the code should read only one file Refunds_20240606.json, then applies a specific schema to the loaded dataframe base on a dictionary schema like {'col1': 'string', 'col2': 'VARCHAR(10)', 'col3': 'FLOAT(64)'} compatible with snowflake's supported data types then writes the dataframe into a parquet file then uploads the parquet file into azure blob storage based an a partition YYYY/MM/DD created from the date in the file name
