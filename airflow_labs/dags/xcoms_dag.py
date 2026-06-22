from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def xcom_push(**kwargs):
    kwargs['ti'].xcom_push(key='file_path', value='/opt/airflow/dags/airflow_labs/data/sample_data.txt')
    return "Pushed file path to XCom"

def xcom_pull(**kwargs):
    path = kwargs['ti'].xcom_pull(key='file_path', task_ids='push_task')
    return f"Pulled file path from XCom: {path}"

with DAG(
    dag_id="xcoms_dag",
    start_date=datetime(2026, 6, 21),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'xcoms']
) as dag:

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=xcom_push
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=xcom_pull
    )

    push_task >> pull_task
