## Table of Contents

1. What is Apache Airflow?
2. Core Concepts
3. Architecture
4. Executors
5. Installing Airflow
6. The Airflow UI
7. Writing Your First DAG
8. Operators
9. XComs — Task Communication
10. Variables
11. Connections
12. Scheduling, Backfill & Catchup
13. Task Groups
14. Dynamic Tasks
15. Complete Pipeline — Reference Implementation

---

## 1. What is Apache Airflow?

Apache Airflow is an **open-source workflow orchestration tool**. Think of it as a smart scheduler and manager for your scripts and data pipelines — it decides *when* to run tasks, in *what order*, and shows you exactly what happened.

### What Airflow does for you

- Automates repetitive tasks (daily reports, data syncs, ETL jobs)
- Defines and enforces dependencies between tasks
- Provides monitoring, logging, and alerting out of the box

### What Airflow is NOT

Airflow is **not an ETL engine** — it does not process or transform data itself. It *orchestrates* the tools that do.

### Why Airflow over drag-and-drop tools?

| Concern | Airflow | GUI Tools (e.g., Azure Data Factory) |
| --- | --- | --- |
| Portability | Workflows are Python code — easy to copy anywhere | Vendor-locked configs |
| Version control | Works natively with Git and CI/CD | Limited or no support |
| Flexibility | Full Python power | Constrained by the GUI |

---

## 2. Core Concepts

Before writing any code, you need to understand three building blocks.

### DAG (Directed Acyclic Graph)

A DAG is the **blueprint** of your workflow — a Python file that defines what tasks exist and in what order they run.

- **Directed** → tasks flow in one direction
- **Acyclic** → no circular dependencies (Task A cannot depend on Task C if Task C depends on Task A)
- **Graph** → tasks are nodes, dependencies are edges

### Task

A **task** is a single, independent unit of work — one node inside a DAG. Examples: "extract data from an API", "run a SQL query", "send an email".

### Operator

An **operator** is a Python class that defines *how* a task runs. You pick the right operator for the job:

| Operator | What it does |
| --- | --- |
| `PythonOperator` | Runs a Python function |
| `BashOperator` | Runs a shell command |
| `PostgresOperator` | Runs a SQL query |
| `FileSensor` | Waits for a file to appear |

### Dependency Syntax

Define the execution order using `>>` (right shift):

```python
task_a >> task_b          # task_b runs after task_a
task_a >> [task_b, task_c]  # task_b and task_c run in parallel after task_a
task_b >> task_d          # task_d runs after task_b
```

**Visual example:**

```
Task A ──┬──► Task B ──► Task D
         │
         └──► Task C
```

Task B and Task C run in parallel. Task D waits for Task B only.

---

## 3. Architecture

Understanding the architecture helps you know what each component does and why it exists.

### Mandatory Components

| Component | Role |
| --- | --- |
| **Metadata Database** | The memory — stores DAG definitions, task states, logs, and retry history |
| **Scheduler** | The decision-maker — parses DAGs and decides when to trigger runs |
| **Executor** | The launcher — decides *how* tasks are executed (locally, via workers, etc.) |

### Optional Components

| Component | Role |
| --- | --- |
| **Webserver** | The UI/API — lets you monitor DAGs and tasks in a browser |
| **Workers** | The runners — execute the actual tasks (needed for Celery/Kubernetes) |
| **DAG Processor** | Offloads DAG file parsing away from the Scheduler |
| **Message Broker** | Transfers tasks from Executor to Workers (e.g., Redis, RabbitMQ) |

### How they connect

```
Scheduler  →  Executor  →  Worker(s)
                  ↑
            Message Broker (if distributed)

All components read/write to the Metadata DB.
Webserver reads the Metadata DB to display results.
```

---

## 4. Executors

The executor determines *how* tasks physically run. Choose based on your environment.

| Executor | How it works | Scalability | Best for |
| --- | --- | --- | --- |
| **Sequential** | One task at a time, no parallelism | Very low | Testing only |
| **Local** | Scheduler and workers on the same machine | Low | Learning / development |
| **Celery** | Tasks sent via message broker to distributed workers | Medium–High | Production workloads |
| **Kubernetes** | Each task gets its own auto-created Pod | Very High | Cloud-native, autoscaling |

### Celery Executor flow

```
Scheduler → Executor → Message Broker (Redis/RabbitMQ) → Workers
```

### Kubernetes Executor flow

```
Scheduler → Executor → Auto-created Pod (one per task)
```

> **Tip for beginners:** Start with the **Local executor** when learning. Move to Celery or Kubernetes only when you need parallelism in production.
> 

---

## 5. Installing Airflow

The recommended method is **Docker Compose** — it spins up all components in isolated containers and is easy to tear down.

### What Docker Compose sets up

```
┌─────────────────────────────────────────────┐
│  Docker Compose                             │
│  ├── Postgres (Metadata DB)                 │
│  ├── Scheduler                              │
│  ├── Executor (Local by default)            │
│  ├── Webserver  (optional but recommended)  │
│  └── Redis / Workers  (if using Celery)     │
└─────────────────────────────────────────────┘
```

The official Airflow Docker Compose file is available at the Apache Airflow documentation.

---

## 6. The Airflow UI

Once the Webserver is running (default: `http://localhost:8080`), you get a dashboard to monitor everything.

### Key views

| View | What you see |
| --- | --- |
| **DAG List** | All DAGs with their status (running / success / failed) |
| **Graph View** | Task dependencies as a visual graph |
| **Grid View** | Task run history across time |
| **Logs** | Full logs per task, per run |

The Webserver does **not** run tasks — it only reads from the Metadata DB and displays results.

---

## 7. Writing Your First DAG

Every DAG is a Python file placed in the `dags/` folder. Airflow automatically picks it up.

### Minimal DAG structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Define the function your task will run
def hello_world():
    print("Hello, Airflow!")

# 2. Define the DAG
with DAG(
    dag_id="first_dag",           # Unique name shown in the UI
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",   # How often to run
    catchup=False                 # Don't run missed past runs
) as dag:

    # 3. Define a task using an Operator
    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )
```

### Important DAG parameters

| Parameter | Purpose |
| --- | --- |
| `dag_id` | Unique identifier, shown in the UI |
| `start_date` | The date from which Airflow considers scheduling this DAG |
| `schedule_interval` | How often to run (cron string, preset, or `timedelta`) |
| `catchup` | If `True`, runs all missed intervals since `start_date`; set `False` for most cases |
| `default_args` | Default settings applied to all tasks (retries, owner, etc.) |

---

## 8. Operators

Operators define *how* a task executes. There are four categories.

### Action Operators — run something

```python
# Run a Python function
from airflow.operators.python import PythonOperator

def my_function():
    print("Running Python!")

python_task = PythonOperator(
    task_id="run_python",
    python_callable=my_function
)
```

```python
# Run a shell command
from airflow.operators.bash import BashOperator

bash_task = BashOperator(
    task_id="bash_demo",
    bash_command="echo 'Hello from Bash!' && date"
)
```

### Transfer Operators — move data

These operators move data between systems (e.g., local → cloud storage).

```python
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

upload_task = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src="/opt/airflow/dags/data/file.csv",
    dst="data/file.csv",
    bucket="my_bucket"
)
```

### Sensor Operators — wait for a condition

Sensors "poke" a condition repeatedly until it's true, then let the next task proceed.

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/opt/airflow/dags/data/input.csv",
    poke_interval=30,   # Check every 30 seconds
    timeout=600         # Fail after 10 minutes
)
```

> **Use case:** Wait for an upstream system to drop a file before your pipeline processes it.
> 

### Specialized / Cloud Operators

Provider packages exist for AWS, GCP, Azure, Databricks, Snowflake, and many more. Install them via `pip install apache-airflow-providers-<name>`.

---

## 9. XComs — Task Communication

XCom (Cross-Communication) lets tasks **pass small pieces of data** to each other. The data is stored in the Metadata Database.

> **Rule of thumb:** XComs are for metadata — file paths, IDs, status flags. Do NOT use them to pass large datasets.
> 

### Pushing data from a task

```python
def extract_data(**context):
    result_path = "/tmp/data.csv"
    # ... do extraction work ...

    # Push a value so other tasks can read it
    context["ti"].xcom_push(key="data_path", value=result_path)
```

### Pulling data in a downstream task

```python
def transform_data(**context):
    # Pull the value pushed by the extract task
    path = context["ti"].xcom_pull(task_ids="extract", key="data_path")
    print(f"Transforming file at: {path}")
```

### Connecting the tasks

```python
from airflow.models.baseoperator import chain

extract_task = PythonOperator(task_id="extract", python_callable=extract_data)
transform_task = PythonOperator(task_id="transform", python_callable=transform_data)

extract_task >> transform_task # Method 1
extract_task.set_downstream(transform_task) # Method 2 `.set_downstream()` or `.set_upstream()`

chain(extract_task, transform_task) # Method 3
```

---

## 10. Variables

Variables are **key-value pairs** stored in the Metadata DB and accessible by any DAG. Use them for environment-specific config that you don't want hardcoded in your DAG files.

### Setting variables

Go to the Airflow UI → **Admin → Variables** → click **+** to add a new variable.

### Reading variables in Python

```python
from airflow.models import Variable

# Simple string
api_key = Variable.get("my_api_key")

# With a fallback default (avoids errors if the variable doesn't exist)
env = Variable.get("ENV", default_var="dev")

# JSON variable — automatically parse it
config = Variable.get("pipeline_config", deserialize_json=True)
```

### Reading in Jinja templates (inside operator arguments)

```python
bash_task = BashOperator(
    task_id="show_env",
    bash_command="echo 'Environment: {{ var.value.ENV }}'"
)
```

---

## 11. Connections

Connections securely store credentials and connection strings for external systems (databases, APIs, cloud). They live in the Metadata DB and are managed via the UI or CLI.

### Setting up a connection

Go to **Admin → Connections** → click **+** → fill in the form with your host, port, login, and password. Give it a `conn_id` (e.g., `my_postgres`).

### Using a connection in an operator

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

query_task = PostgresOperator(
    task_id="run_query",
    postgres_conn_id="my_postgres",   # References the conn_id you created in the UI
    sql="SELECT * FROM users LIMIT 10;"
)
```

### Accessing a connection in Python code

```python
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("my_postgres")
print(conn.host, conn.port, conn.login)
```

---

## 12. Scheduling, Backfill & Catchup

### Schedule intervals

You can define how often a DAG runs in three ways:

**Preset strings**

| Preset | Meaning |
| --- | --- |
| `@once` | Run exactly once |
| `@hourly` | Every hour |
| `@daily` | Every day at midnight |
| `@weekly` | Every Monday at midnight |
| `@monthly` | 1st of every month |

**Cron expressions**

```
"0 12 * * *"    → every day at 12:00
"0 */2 * * *"   → every 2 hours
"30 6 * * 1-5"  → 6:30 AM on weekdays only
```

**Python timedelta**

```python
from datetime import timedelta

schedule_interval=timedelta(hours=6)  # Every 6 hours
```

### Catchup

When `catchup=True`, Airflow automatically schedules all missed DAG runs between `start_date` and now. This is useful when you first deploy a DAG with a past `start_date`.

```python
with DAG(
    dag_id="my_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=True   # Will create runs for all days since Jan 1
) as dag:
    ...
```

> **Best practice:** Set `catchup=False` for most DAGs to avoid accidentally triggering hundreds of historical runs on first deploy.
> 

### Backfill (manual)

To manually trigger runs for a past date range from the CLI:

```bash
airflow dags backfill -s 2026-01-01 -e 2026-01-31 my_dag
```

| Concept | Trigger | Use case |
| --- | --- | --- |
| **Catchup** | Automatic on deploy | Re-process missed scheduled runs |
| **Backfill** | Manual CLI command | Load historical data on demand |

---

## 13. Task Groups

A Task Group **logically groups related tasks** inside a DAG. It doesn't change execution order — it just collapses multiple tasks into one expandable node in the UI, keeping complex DAGs readable.

### When to use Task Groups

- You have an ETL pipeline with many tasks per stage (extract, transform, load)
- You want to visually separate stages in the Graph view
- You have nested multi-stage pipelines

### Code example

```python
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="task_group_demo",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    # All three tasks are grouped under "etl_tasks"
    with TaskGroup("etl_tasks") as etl_group:
        extract = BashOperator(
            task_id="extract",
            bash_command="echo 'Extracting data'"
        )
        transform = BashOperator(
            task_id="transform",
            bash_command="echo 'Transforming data'"
        )
        load = BashOperator(
            task_id="load",
            bash_command="echo 'Loading data'"
        )
        extract >> transform >> load  # Order within the group

    notify = BashOperator(
        task_id="notify",
        bash_command="echo 'ETL Complete!'"
    )

    etl_group >> notify  # Entire group must finish before notify
```

### What this looks like in the UI

```
[ ETL Tasks Group ]
  ├── extract
  ├── transform
  └── load
       ↓
    notify
```

---

## 14. Dynamic Tasks

Dynamic tasks are generated **at runtime** based on a list of inputs. Instead of writing one task per file (or per country, or per partition), you let Airflow expand a single task definition into many parallel instances.

This uses the **TaskFlow API** (`@task` decorator) and `.expand()`, available in Airflow 2.3+.

### When to use Dynamic Tasks

- Processing a variable number of files
- Running the same logic across multiple regions/customers/partitions
- Avoiding repetitive, hardcoded task definitions

### Code example

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="dynamic_task_example",
    start_date=datetime(2026, 1, 1),
    schedule=None,    # Triggered manually
    catchup=False
)
def dynamic_task_example():

    @task
    def get_files():
        # In production, this might list files from S3 or a database
        return ["file1.csv", "file2.csv", "file3.csv"]

    @task
    def process_file(file_name: str):
        print(f"Processing: {file_name}")

    files = get_files()
    process_file.expand(file_name=files)  # Creates one task instance per file

dynamic_task_example()
```

### What this looks like at runtime

```
get_files()
    ↓
process_file(file1.csv)   ←─┐
process_file(file2.csv)   ←─┤  All run in parallel
process_file(file3.csv)   ←─┘
```

### Task Groups vs Dynamic Tasks

| Concept | Purpose | Key benefit |
| --- | --- | --- |
| **Task Group** | Organize known tasks for readability | Cleaner UI, no runtime logic |
| **Dynamic Task** | Generate tasks at runtime from data | Scalable, avoids hardcoding |

---

## 15. Complete Pipeline — Reference Implementation

This is a full working DAG that demonstrates every concept covered in this guide. Read through it as a reference after completing the sections above.

```python
from datetime import datetime, timedelta
import json
import random
import time

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────────────────────
# VARIABLES & CONNECTIONS
# Set these up in the UI before running:
#   Admin → Variables  →  ENV = "dev"
#   Admin → Connections → conn_id = "my_postgres"
# ─────────────────────────────────────────────────────────────

ENV = Variable.get("ENV", default_var="dev")
postgres_conn = BaseHook.get_connection("my_postgres")
print("DB Host:", postgres_conn.host)

# ─────────────────────────────────────────────────────────────
# DEFAULT ARGS — applied to every task in the DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "depends_on_past": False,
}

# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="masterclass_complete_pipeline",
    description="Complete Airflow Masterclass DAG",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",   # Every 5 minutes
    catchup=False,
    default_args=default_args,
    tags=["masterclass", "etl", "dynamic"],
) as dag:

    # ── START ────────────────────────────────────────────────

    start = BashOperator(
        task_id="start",
        bash_command='echo "Pipeline Started"',
    )

    # ── SENSOR — wait 10 seconds before proceeding ───────────

    wait_10_seconds = TimeDeltaSensor(
        task_id="wait_10_seconds",
        delta=timedelta(seconds=10),
    )

    # ── EXTRACT — push data via XCom ─────────────────────────

    def extract_data(**context):
        data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        }
        print("Extracted:", data)
        context["ti"].xcom_push(key="raw_data", value=data)
        return data

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    # ── TRANSFORM — pull data via XCom ───────────────────────

    def transform_data(**context):
        data = context["ti"].xcom_pull(task_ids="extract", key="raw_data")
        transformed = [
            {"user_id": u["id"], "user_name": u["name"].upper()}
            for u in data["users"]
        ]
        print("Transformed:", transformed)
        return transformed

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    # ── BASH OPERATOR DEMO ────────────────────────────────────

    bash_demo = BashOperator(
        task_id="bash_demo",
        bash_command="""
        echo "Running Bash Operator"
        date
        """,
    )

    # ── TASK GROUP — validate + enrich ───────────────────────

    with TaskGroup(group_id="processing_group") as processing_group:

        def validate():
            print("Validating data...")
            time.sleep(2)

        def enrich():
            print("Enriching data...")
            time.sleep(2)

        validate_task = PythonOperator(
            task_id="validate_task",
            python_callable=validate,
        )
        enrich_task = PythonOperator(
            task_id="enrich_task",
            python_callable=enrich,
        )
        validate_task >> enrich_task

    # ── DYNAMIC TASKS — one task per country ─────────────────

    @task
    def get_api_data():
        return [
            {"country": "India", "sales": 100},
            {"country": "USA",   "sales": 200},
            {"country": "UK",    "sales": 300},
        ]

    @task
    def process_country(record):
        result = {
            "country": record["country"],
            "processed_sales": record["sales"] * 10,
        }
        print(result)
        return result

    api_data = get_api_data()
    dynamic_processing = process_country.expand(record=api_data)

    # ── CLOUD UPLOAD SIMULATION ───────────────────────────────

    @task
    def upload_to_cloud():
        print("Uploading to S3 / GCS / Azure Blob...")
        time.sleep(3)
        return "upload_success"

    upload = upload_to_cloud()

    # ── LOAD — read transformed data and write to warehouse ───

    def load_data(**context):
        transformed = context["ti"].xcom_pull(task_ids="transform")
        print("Loading to warehouse:")
        print(json.dumps(transformed, indent=4))
        print("Done.")

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    # ── QUALITY CHECK ─────────────────────────────────────────

    @task
    def quality_check():
        score = random.randint(80, 100)
        print(f"Quality Score: {score}")
        if score < 85:
            raise ValueError("Quality Check Failed!")
        return "Quality Passed"

    quality = quality_check()

    # ── NOTIFICATION — only runs if everything succeeded ──────

    def notify():
        print("Sending Success Notification")

    notification = PythonOperator(
        task_id="notification",
        python_callable=notify,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── END ───────────────────────────────────────────────────

    end = BashOperator(
        task_id="end",
        bash_command='echo "Pipeline Completed"',
    )

    # ── PIPELINE ORDER ────────────────────────────────────────

    (
        start
        >> wait_10_seconds
        >> extract
        >> transform
        >> bash_demo
        >> processing_group
        >> api_data
        >> dynamic_processing
        >> upload
        >> load
        >> quality
        >> notification
        >> end
    )
```

### Pipeline flow at a glance

```
start
  → wait_10_seconds  (Sensor)
  → extract          (PythonOperator + XCom push)
  → transform        (PythonOperator + XCom pull)
  → bash_demo        (BashOperator)
  → processing_group (TaskGroup: validate → enrich)
  → get_api_data     (TaskFlow)
  → process_country  (Dynamic × 3: India, USA, UK)
  → upload_to_cloud  (TaskFlow)
  → load             (PythonOperator + XCom pull)
  → quality_check    (TaskFlow, random pass/fail)
  → notification     (runs only on ALL_SUCCESS)
  → end
```

---

## Quick Reference

| Concept | One-liner |
| --- | --- |
| DAG | Python blueprint of your workflow |
| Task | A single unit of work in a DAG |
| Operator | The class that defines how a task runs |
| Executor | Determines where/how tasks are physically run |
| XCom | Pass small values (IDs, paths) between tasks |
| Variable | Global key-value config stored in the metadata DB |
| Connection | Stored credentials for external systems |
| Catchup | Auto-run missed past intervals on deploy |
| Backfill | Manually trigger runs for past date ranges |
| Task Group | Visual grouping of tasks in the UI |
| Dynamic Task | Generate tasks at runtime using `.expand()` |