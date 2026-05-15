# Celery Setup & Worker Management Guide

## 1. Basic Celery Setup

First, create a Python file with Celery initialized and wrap functions using the `@app.task` decorator.

### Example: `celery_app.py`

```python
from celery import Celery

app = Celery("tasks")

app.conf.broker_url = "amqp://username:password@rabbitmq:5672//"
app.conf.result_backend = "redis://redis:6379/1"


@app.task
def add(x, y):
    return x + y
```

---

## 2. Docker & Worker Commands

### Open Docker Shell

```bash
docker exec -it celery_worker bash
```

### Run Celery Worker

```bash
celery -A celery_app worker --loglevel=info -E
```

### Test Run a Celery Task

```bash
docker exec celery_worker python -c "from celery_app import add; print(add.delay(10, 20))"
```

---

# Task Calling Methods

## Using `.delay()`

```python
add.delay(2, 2)
```

## Using `.apply_async()`

Supports advanced configurations such as:

* Queue priority
* Delayed execution (`countdown`)

```python
add.apply_async((2, 2), queue='lopri', countdown=10)
```

---

# Result Backend Persistence

To persist task execution results, configure a backend such as Redis.

### Broker URL (RabbitMQ)

> `broker_url` variable name is fixed.

```python
broker_url = "amqp://username:password@rabbitmq:5672//"
```

### Result Backend (Redis)

> `result_backend` variable name is fixed.

```python
result_backend = "redis://redis:6379/1"
```

---

# Shared Tasks

Instead of binding tasks to a specific app instance, Celery supports shared tasks.

```python
from celery import shared_task

@shared_task
def add(x, y):
    return x + y
```

---

# Task Priority Configuration

```python
task_routes = {
    'tasks.math.add': {'priority': 9},
    'tasks.math.mul': {'priority': 0},
}
```

---

# Task Annotations (Rate Limiting)

```python
task_annotations = {
    'tasks.math.square': {'rate_limit': '2/m'}
}
```

---

# Understanding Workers

Adding workers can mean two things:

1. Increasing concurrency
2. Running separate workers for different task types

---

# Celery Beat (Task Scheduler)

## Run Celery Beat

```bash
celery -A celery_app beat --loglevel=info
```

## What `celerybeat-schedule` Stores

* Last execution timestamp
* Next scheduled run
* Task metadata
* Schedule hash

---

# Debugging Scheduled Tasks

## Redis CLI Command

```bash
LRANGE celery 0 -1
```

---

# Concurrency Models in Celery

Concurrency defines how Celery executes tasks.

---

## A. Prefork (Default)

Uses multiple OS processes.

### Architecture

```text
Master process
   ↓ forks
Worker 1 (process)
Worker 2 (process)
Worker 3 (process)
```

```text
Worker Master
 ├── Process 1 → task
 ├── Process 2 → task
 ├── Process 3 → task
 └── Process 4 → task
```

### CLI Command

```bash
celery -A celery_app worker --pool=prefork --concurrency=4
```

---

## B. Threads

Uses Python threads instead of processes.

### CLI Command

```bash
celery -A celery_app worker --pool=threads --concurrency=10
```

---

## C. Gevent

Uses cooperative lightweight threads called greenlets.

### CLI Command

```bash
celery -A celery_app worker --pool=gevent --concurrency=100
```

---

## D. Eventlet

Similar to Gevent but with an older ecosystem.

### CLI Command

```bash
celery -A celery_app worker --pool=eventlet --concurrency=100
```

---

# Advanced Worker Configuration

```bash
celery -A celery_app worker \
  --pool=prefork \
  --concurrency=4 \
  --autoscale=10,3 \
  --prefetch-multiplier=1 \
  --max-tasks-per-child=100 \
  --max-memory-per-child=20000
```

## Configuration Breakdown

| Option                   | Description                  |
| ------------------------ | ---------------------------- |
| `--pool`                 | Concurrency model            |
| `--concurrency`          | Number of workers            |
| `--autoscale`            | Dynamic scaling (`max,min`)  |
| `--prefetch-multiplier`  | Reserved tasks per worker    |
| `--max-tasks-per-child`  | Restart worker after N tasks |
| `--max-memory-per-child` | Memory limit per worker      |

---

# Kombu Message Abstraction Layer

Kombu acts as the messaging abstraction layer between Celery and brokers like Redis/RabbitMQ.

```python
from kombu import Queue
```

## Custom Queue Configuration

```python
task_queues = {
    Queue('delayed_task_queue'),
}
```

---

# Running Workers for Specific Queues

## Delayed Task Queue Worker

```bash
celery -A celery_app worker \
  -n worker1@%h \
  -Q delayed_task_queue \
  --loglevel=info \
  -E \
  -n delayed-custom-queue \
  --pool=prefork \
  --concurrency=2 \
  --prefetch-multiplier=2 \
  --max-tasks-per-child=100 \
  --max-memory-per-child=30000
```

## ETL Queue Worker

```bash
celery -A celery_app worker \
  -n worker2@%h \
  -Q etl_queue \
  --loglevel=info \
  -E \
  -n etl-custom-queue
```

## Retry Queue Worker

```bash
celery -A celery_app worker \
  -n worker3@%h \
  -Q retry_task_queue \
  --loglevel=info \
  -E \
  -n retry_task_queue
```

---

# Retry Mechanisms

## Manual Retry

```python
except Exception as e:
    raise self.retry(exc=e, countdown=60, max_retries=3)
```

## Automatic Retry

```python
@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5, 'countdown': 10},
    retry_backoff=True
)
def retry_task(self):
    pass
```

---

# Logging in Celery

```python
import logging

logger = logging.getLogger(__name__)

logger.info("This is an info log from the log_task.")
logger.warning("This is a warning log from the log_task.")
logger.error("This is an error log from the log_task.")
```

---

# Useful Shell Scripts

```bash
cd celery_labs && source venv/bin/activate

celery -A celery_app worker -n worker6@%h --loglevel=info -E -n idle_worker

celery -A celery_app worker -n log_task_worker4@%h -Q log_task_queue --loglevel=info -E

celery -A celery_app worker -n retry_task_worker2@%h -Q retry_task_queue --loglevel=info -E

celery -A celery_app worker -n etl_queue_worker1@%h -Q etl_queue --loglevel=info -E

celery -A celery_app worker \
  -n worker3@%h \
  -Q delayed_task_queue \
  --loglevel=info \
  -E \
  -n delayed-custom-queue \
  --pool=prefork \
  --concurrency=2 \
  --prefetch-multiplier=2 \
  --max-tasks-per-child=100 \
  --max-memory-per-child=30000

celery -A celery_app worker \
  -n math_queue_worker5@%h \
  -Q math_task_queue \
  --loglevel=info -E

celery -A celery_app worker \
  --pool=prefork \
  --concurrency=4 \
  --autoscale=10,3 \
  --prefetch-multiplier=1 \
  --max-tasks-per-child=100 \
  --max-memory-per-child=200000

docker exec -it celery_worker python

docker exec -it redis redis-cli

docker exec -it celery_worker bash

celery -A celery_app beat --loglevel=info

cd celery_labs && source venv/bin/activate
```

---

# Python Shell Testing Code

```python
from celery import chain
from tasks.math import add, mul, square
from tasks.delay import delay_task
from tasks.etl import ingestion_task, transformation_task, loading_task
from tasks.logger import log_task
from tasks.retries import retry_task
import time


# Loop to call add task asynchronously 100 times
for i in range(100):
    result = add.delay(i, i + 1)
    print(f"add task id: {result.id}")
    time.sleep(10)


# Loop to call mul task asynchronously 100 times
for i in range(100):
    result = mul.delay(i, i + 2)
    print(f"mul task id: {result.id}")
    time.sleep(10)


# Calling square task once
print(square.delay(9))


# Loop to call square task multiple times
for i in range(10):
    print(square.delay(i * i))


# Delay task with 800ms
for i in range(20):
    print(delay_task.delay(800))


# Delay task with 1000ms
for i in range(100):
    print(delay_task.delay(1000))


# ETL pipeline using task chaining
for i in range(10):
    result = chain(
        ingestion_task.s(),
        transformation_task.s(),
        loading_task.s()
    ).apply_async()

    print(result)
    time.sleep(10)


# Logging task loop
for i in range(10000):
    result = log_task.delay()
    print(result.id)
    time.sleep(5 * i)


# Retry task execution
for _ in range(150):
    res = retry_task.delay()
    print("task_id:", res.id)

    while True:
        res_state = res.state
        print("state:", res_state)

        if res_state in ("SUCCESS", "FAILURE"):
            break

        time.sleep(2)

    print("final:", res.state)

    if res.successful():
        print("result:", res.result)
    else:
        print("error:", res.result)
```

---

# Prometheus Queries

```promql
{__name__=~"rabbitmq_queue_messages.*"}
```

```promql
{__name__=~"celery_.*"}
```

```promql
{__name__=~"redis_memory_.*"}
```

```promql
{__name__=~"celery_.*|rabbitmq_queue_.*|redis_memory_.*"}
```

---

# Grafana Queries

```text
the-data-stack-lab/celery_labs/GRAFANAQUERY.json
```