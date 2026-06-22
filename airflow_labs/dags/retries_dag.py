from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.utils.email import send_email

def success_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    return f"Task {task_id} in DAG {dag_id} succeeded."

def failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    exception = context.get("exception")
    return f"Task {task_id} in DAG {dag_id} failed."

    send_email(
        to="abhirajkviit@gmail.com",
        subject=f"Airflow Task Failure: {task_id} in DAG {dag_id}",
        html_content=f"""<p>Task <b>{task_id}</b> in DAG <b>{dag_id}</b> has failed.</p>
                        <p>Exception: {exception}</p>"""
    )

def risky_task():
    import random
    if random.choice([True, False]):
        raise ValueError("Random failure occurred!")
    return "Task completed successfully."

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'on_success_callback': success_callback,
    'on_failure_callback': failure_callback,
    'email_on_failure': True,
    'email': ['abhirajkviit@gmail.com']
}

with DAG(
    dag_id = "retries_dag",
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '*/2 * * * *',
    catchup = False,
    tags =['retries', 'example']
) as dag:
    
    task1 = PythonOperator(
        task_id = 'risky_task',
        python_callable = risky_task
    )

