from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime
import requests

def hello_airflow():
    print("Hello, Airflow! This is a DAG with a graph.")
    print("Raising an AirflowException error to show fail task")
    raise AirflowException("Forced Failed Task")

def call_api(latitude, longitude):
    api = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={latitude}&longitude={longitude}&hourly=birch_pollen,grass_pollen"
    try:
        response = requests.get(api)
        response.raise_for_status()
        data = response.json()
        print(f"Air Quality Data for Latitude: {latitude}, Longitude: {longitude}")
        print(data)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

with DAG(
    dag_id = "failed_dag",
    start_date = datetime(2026, 6, 6),
    schedule_interval = '0 */2 * * *',
    catchup = False,
    tags = ['example', 'failed']
)as dag:
    
    task1 = PythonOperator(
        task_id = 'hello_airflow',
        python_callable = hello_airflow
    )

    task2 = PythonOperator(
        task_id = "call_api",
        python_callable = call_api,
        op_kwargs = {
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    )

    task1 >> task2