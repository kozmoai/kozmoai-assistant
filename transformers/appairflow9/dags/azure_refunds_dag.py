from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from azure_file_share_hook import AzureFileShareHook
from utils import (
    list_json_files,
    copy_files_to_local,
    read_latest_file,
    apply_schema,
    write_to_parquet,
    upload_to_blob_storage,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "azure_refunds_dag",
    default_args=default_args,
    description="A DAG to process refund JSON files from Azure File Share",
    schedule_interval=timedelta(days=1),
)

azure_conn_id = "azure_default"
share_name = "your-share-name"
directory_name = "your-directory-name"
local_path = "/tmp/airflow/refunds"
schema = {"col1": "string", "col2": "VARCHAR(10)", "col3": "FLOAT(64)"}

list_files_task = PythonOperator(
    task_id="list_json_files",
    python_callable=list_json_files,
    op_kwargs={
        "azure_conn_id": azure_conn_id,
        "share_name": share_name,
        "directory_name": directory_name,
    },
    dag=dag,
)

copy_files_task = PythonOperator(
    task_id="copy_files_to_local",
    python_callable=copy_files_to_local,
    op_kwargs={
        "azure_conn_id": azure_conn_id,
        "share_name": share_name,
        "directory_name": directory_name,
        "local_path": local_path,
    },
    dag=dag,
)

read_latest_file_task = PythonOperator(
    task_id="read_latest_file",
    python_callable=read_latest_file,
    op_kwargs={"local_path": local_path},
    dag=dag,
)

apply_schema_task = PythonOperator(
    task_id="apply_schema",
    python_callable=apply_schema,
    op_kwargs={"schema": schema},
    dag=dag,
)

write_to_parquet_task = PythonOperator(
    task_id="write_to_parquet",
    python_callable=write_to_parquet,
    op_kwargs={"local_path": local_path},
    dag=dag,
)

upload_to_blob_storage_task = PythonOperator(
    task_id="upload_to_blob_storage",
    python_callable=upload_to_blob_storage,
    op_kwargs={"local_path": local_path, "azure_conn_id": azure_conn_id},
    dag=dag,
)

(
    list_files_task
    >> copy_files_task
    >> read_latest_file_task
    >> apply_schema_task
    >> write_to_parquet_task
    >> upload_to_blob_storage_task
)
