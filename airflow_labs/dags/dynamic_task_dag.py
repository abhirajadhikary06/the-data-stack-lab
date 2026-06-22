from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id="dynamic_task_dag",
    start_date=datetime(2026, 6, 21),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'dynamic'],
    default_args={
        'owner': 'airflow',
        'retries': 10,
        'retry_delay': timedelta(seconds=10),
    }
)

def dynamic_task_example():
    @task
    def get_files():
        return ['file1.txt', 'file2.txt', 'file3.txt']

    @task
    def process_file(file_name: str):
        return f"Processed {file_name}"

    files = get_files()
    process_file.expand(file_name=files)

dynamic_task_example()