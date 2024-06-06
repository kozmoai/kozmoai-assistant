from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from azure_file_share_hook import AzureFileShareHook
from azure_blob_storage_hook import AzureBlobStorageHook
import pandas as pd
import yaml
import re
import os

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
    description="A DAG to process Refunds JSON files from Azure File Share and write to Azure Blob Storage",
    schedule_interval=timedelta(days=1),
)


def list_json_files(**kwargs):
    azure_file_share_hook = AzureFileShareHook()
    files = azure_file_share_hook.list_files()
    json_files = [f for f in files if re.match(r"^Refunds_.*\.json$", f)]
    return json_files


def read_latest_file(json_files, **kwargs):
    latest_file = max(
        json_files, key=lambda f: re.search(r"Refunds_(\d{8})\.json$", f).group(1)
    )
    azure_file_share_hook = AzureFileShareHook()
    file_content = azure_file_share_hook.read_file(latest_file)
    return latest_file, file_content


def apply_schema(file_content, **kwargs):
    schema_path = os.path.join(os.path.dirname(__file__), "schema.yaml")
    with open(schema_path, "r") as schema_file:
        schema = yaml.safe_load(schema_file)
    df = pd.read_json(file_content)
    df = df.astype(schema)
    return df


def write_to_parquet(df, latest_file, **kwargs):
    date_str = re.search(r"Refunds_(\d{8})\.json$", latest_file).group(1)
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    partition_path = f"{year}/{month}/{day}"
    azure_blob_storage_hook = AzureBlobStorageHook()
    azure_blob_storage_hook.write_parquet(df, partition_path)


list_files_task = PythonOperator(
    task_id="list_json_files",
    python_callable=list_json_files,
    provide_context=True,
    dag=dag,
)

read_latest_file_task = PythonOperator(
    task_id="read_latest_file",
    python_callable=read_latest_file,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids="list_json_files") }}'],
    dag=dag,
)

apply_schema_task = PythonOperator(
    task_id="apply_schema",
    python_callable=apply_schema,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids="read_latest_file")[1] }}'],
    dag=dag,
)

write_to_parquet_task = PythonOperator(
    task_id="write_to_parquet",
    python_callable=write_to_parquet,
    provide_context=True,
    op_args=[
        '{{ ti.xcom_pull(task_ids="apply_schema") }}',
        '{{ ti.xcom_pull(task_ids="read_latest_file")[0] }}',
    ],
    dag=dag,
)

list_files_task >> read_latest_file_task >> apply_schema_task >> write_to_parquet_task
