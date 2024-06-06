from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from plugins.utils.file_utils import list_json_files, get_latest_file, read_file

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def list_and_read_latest_file(**kwargs):
    path = kwargs["path"]
    files = list_json_files(path)
    latest_file = get_latest_file(files)
    content = read_file(latest_file)
    print(content)


with DAG(
    "azure_file_share_dag",
    default_args=default_args,
    description="A simple DAG to list and read the latest JSON file from Azure File Share",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="list_and_read_latest_file",
        python_callable=list_and_read_latest_file,
        op_kwargs={"path": "your/azure/file/share/path"},
    )

    task
