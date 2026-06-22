from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="taskgroup_dag",
    start_date=datetime(2026, 6, 21),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'taskgroup']
) as dag:

    start_task = BashOperator(
        task_id="start",
        bash_command="echo 'Starting the DAG'"
    )
    
    with TaskGroup("etl_task") as etl_group:
        extract=BashOperator(
            task_id="extract",
            bash_command="echo 'Performing data extraction'"
        )
        transform=BashOperator(
            task_id="transform",
            bash_command="echo 'Performing data transformation'"
        )
        load=BashOperator(
            task_id="load",
            bash_command="echo 'Loading data into the destination'"
        )

        extract >> transform >> load
    
    notify_task = BashOperator(
        task_id="notify",
        bash_command="echo 'ETL process completed. Sending notification.'"
    )

    start_task >> etl_group >> notify_task