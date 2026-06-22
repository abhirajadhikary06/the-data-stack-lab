from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from datetime import datetime
import os
from openai import OpenAI

# Define the connection helper outside to keep tasks clean
def get_openai_client():
    api_key = Variable.get("groq_api_key", default_var=os.getenv("GROQ_API_KEY"))
    return OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")

@dag(
    dag_id="taskgroup_dynamic_task_with_variable_dag",
    start_date=datetime(2026, 6, 21),
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'taskgroup', 'dynamic', 'variable'],
)
def ai_pipeline():

    @task
    def check_api_key():
        api_key = Variable.get("groq_api_key", default_var=os.getenv("GROQ_API_KEY"))
        if not api_key:
            raise ValueError("API key is not set.")

    @task_group(group_id="ai_etl_task")
    def ai_etl_group():
        @task
        def extract():
            client = get_openai_client()
            response = client.chat.completions.create(
                messages=[{"role": "user", "content": "Explain the importance of fast language models"}],
                model="llama-3.3-70b-versatile",
            )
            return response.choices[0].message.content

        @task
        def transform(text: str):
            return f"Transformed data: {text}"

        @task
        def load(text: str):
            print(f"Loaded: {text}")

        # TaskFlow automatically handles passing data between tasks
        content = extract()
        load(transform(content))

    # --- Dynamic Task Implementation ---
    @task
    def get_prompts():
        return [
            "Explain the importance of fast language models",
            "What are the benefits of using AI in data processing?",
            "How can AI improve decision-making in businesses?"
        ]

    @task
    def api_call(message: str):
        client = get_openai_client()
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": message}],
            model="llama-3.3-70b-versatile",
        )
        return response.choices[0].message.content

    # This creates 3 tasks dynamically
    messages = get_prompts()
    api_call.expand(message=messages)

    # Run the setup check first
    check_api_key() >> ai_etl_group()

# Invoke the DAG
ai_pipeline()