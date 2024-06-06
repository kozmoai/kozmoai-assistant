from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from azure_file_share_hook import AzureFileShareHook
from azure_blob_storage_hook import AzureBlobStorageHook
import pandas as pd
import pyarrow.parquet as pq
import re
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "azure_file_share_to_blob",
    default_args=default_args,
    description="A DAG to process JSON files from Azure File Share and upload to Azure Blob Storage",
    schedule_interval=timedelta(days=1),
)


def list_json_files():
    hook = AzureFileShareHook()
    return hook.list_files()


def copy_files_to_local(file_list):
    hook = AzureFileShareHook()
    for file in file_list:
        hook.download_file(file, "/tmp/airflow/")


def read_latest_file():
    files = [
        f for f in os.listdir("/tmp/airflow/") if re.match(r"Refunds_\d{8}\.json", f)
    ]
    latest_file = max(
        files, key=lambda x: datetime.strptime(re.search(r"\d{8}", x).group(), "%Y%m%d")
    )
    return latest_file


def apply_schema(file_name):
    schema = {"col1": "string", "col2": "VARCHAR(10)", "col3": "FLOAT(64)"}
    df = pd.read_json(f"/tmp/airflow/{file_name}")
    for col, dtype in schema.items():
        if dtype.startswith("VARCHAR"):
            df[col] = df[col].astype(str)
        elif dtype.startswith("FLOAT"):
            df[col] = df[col].astype(float)
    return df


def write_parquet(df, file_name):
    date_str = re.search(r"\d{8}", file_name).group()
    date = datetime.strptime(date_str, "%Y%m%d")
    partition_path = date.strftime("%Y/%m/%d")
    parquet_file = f"/tmp/airflow/{partition_path}/data.parquet"
    os.makedirs(os.path.dirname(parquet_file), exist_ok=True)
    df.to_parquet(parquet_file)
    return parquet_file


def upload_to_blob_storage(parquet_file):
    hook = AzureBlobStorageHook()
    hook.upload_file(
        parquet_file, "my-container", parquet_file.replace("/tmp/airflow/", "")
    )


list_files_task = PythonOperator(
    task_id="list_json_files",
    python_callable=list_json_files,
    dag=dag,
)

copy_files_task = PythonOperator(
    task_id="copy_files_to_local",
    python_callable=copy_files_to_local,
    op_args=['{{ ti.xcom_pull(task_ids="list_json_files") }}'],
    dag=dag,
)

read_latest_file_task = PythonOperator(
    task_id="read_latest_file",
    python_callable=read_latest_file,
    dag=dag,
)

apply_schema_task = PythonOperator(
    task_id="apply_schema",
    python_callable=apply_schema,
    op_args=['{{ ti.xcom_pull(task_ids="read_latest_file") }}'],
    dag=dag,
)

write_parquet_task = PythonOperator(
    task_id="write_parquet",
    python_callable=write_parquet,
    op_args=[
        '{{ ti.xcom_pull(task_ids="apply_schema") }}',
        '{{ ti.xcom_pull(task_ids="read_latest_file") }}',
    ],
    dag=dag,
)

upload_to_blob_storage_task = PythonOperator(
    task_id="upload_to_blob_storage",
    python_callable=upload_to_blob_storage,
    op_args=['{{ ti.xcom_pull(task_ids="write_parquet") }}'],
    dag=dag,
)

(
    list_files_task
    >> copy_files_task
    >> read_latest_file_task
    >> apply_schema_task
    >> write_parquet_task
    >> upload_to_blob_storage_task
)
