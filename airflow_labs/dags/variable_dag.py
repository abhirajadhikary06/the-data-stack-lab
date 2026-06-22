from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from openai import OpenAI

def query_groq_api():
    # Retrieve the variable directly inside the function
    # This is more reliable than relying on top-level environment loads
    api_key = Variable.get("groq_api_key")
    
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.groq.com/openai/v1",
    )
    response = client.chat.completions.create(
        messages=[
            {"role": "user", "content": "Explain the importance of fast language models"}
        ],
        model="llama-3.3-70b-versatile",
    )
    return response.choices[0].message.content

with DAG(
    dag_id="variable_dag",
    start_date=datetime(2026, 6, 21),
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['example', 'variables']
) as dag:

    query_groq_task = PythonOperator(
        task_id="query_groq_api",
        python_callable=query_groq_api
    )
