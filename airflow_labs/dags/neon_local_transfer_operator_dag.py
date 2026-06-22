from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv

def export_from_neon():
    output_path = "/opt/airflow/dags/airflow_labs/data/employees_data.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    hook = PostgresHook(postgres_conn_id="neon_db")
    sql = "SELECT * FROM employees;"
    records = hook.get_records(sql)
    with open(output_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(['id', 'name', 'email'])  # Write header if file is empty
        writer.writerows(records)

'''
def import_to_neon():
    hook = PostgresHook(postgres_conn_id="neon_db")
    input_path = "/opt/airflow/dags/airflow_labs/data/employees_data.csv"
    with open(input_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            sql = "INSERT INTO employees (id, name, email) VALUES (%s, %s, %s);"
            hook.run(sql, parameters=(row['id'], row['name'], row['email']))

'''
with DAG(
    dag_id = "neon_local_transfer_operator_dag",
    start_date = datetime(2026, 6, 21),
    schedule_interval = None,
    catchup = False,
    tags = ['example', 'neon', 'transfer']
) as dag:

    export_task = PythonOperator(
        task_id = "export_from_neon",
        python_callable = export_from_neon
    )

    # import_task = PythonOperator(
    #     task_id = "import_to_neon",
    #     python_callable = import_to_neon
    # )

    export_task 
    # >> import_task
