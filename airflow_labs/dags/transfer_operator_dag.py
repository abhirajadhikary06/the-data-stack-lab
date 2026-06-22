from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import shutil 

def copy_local_file():
    src = Path("/opt/airflow/dags/airflow_labs/data/sample_data.txt")
    dst = Path("/opt/airflow/dags/airflow_labs/data/sample_data_copy.txt")
    if not src.exists():
        raise FileNotFoundError(f"Source file {src} does not exist.")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)
    print(f"Copied {src} to {dst}") 

with DAG(
    dag_id = "transfer_operator_dag",
    start_date = datetime(2026, 6, 21),
    schedule_interval = '@daily',
    catchup = False,
    tags = ['example', 'transfer']
) as dag:

     copy_file_task = PythonOperator(
        task_id = "copy_local_file",
        python_callable = copy_local_file
    )
