from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from hooks.azure_file_share_hook import AzureFileShareHook
from hooks.azure_blob_storage_hook import AzureBlobStorageHook
from utils.file_operations import list_json_files, copy_files_to_local, read_latest_file
from utils.dataframe_operations import apply_schema, write_to_parquet

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "refunds_processing_dag",
    default_args=default_args,
    description="A DAG to process refund JSON files",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


def process_refunds():
    azure_file_share_hook = AzureFileShareHook(conn_id="azure_file_share_default")
    azure_blob_storage_hook = AzureBlobStorageHook(conn_id="azure_blob_storage_default")

    # List JSON files
    json_files = list_json_files(
        azure_file_share_hook, "path/to/fileshare", r"^Refunds_.*\.json$"
    )

    # Copy files to local
    local_files = copy_files_to_local(
        azure_file_share_hook, json_files, "/tmp/airflow/refunds"
    )

    # Read the latest file
    latest_file = read_latest_file(local_files, r"^Refunds_(\d{8})\.json$")

    # Apply schema
    schema = {"col1": "string", "col2": "VARCHAR(10)", "col3": "FLOAT(64)"}
    df = apply_schema(latest_file, schema)

    # Write to Parquet
    parquet_file = write_to_parquet(df, "/tmp/airflow/refunds/output.parquet")

    # Upload to Blob Storage
    upload_to_blob_storage(azure_blob_storage_hook, parquet_file, "path/to/blobstorage")


process_refunds_task = PythonOperator(
    task_id="process_refunds",
    python_callable=process_refunds,
    dag=dag,
)

process_refunds_task
