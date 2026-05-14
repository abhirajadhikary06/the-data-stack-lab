First need to have a python file with Celery initialized and python function enclosed under `@app.task` decorator
Docker Shell - `docker exec -it celery_worker bash`
Run Celery Worker - `celery -A celery_app worker --loglevel=info -E`
Test Run the task created in celery_app.py - `docker exec celery_worker python -c "from celery_app import add; print(add.delay(10, 20))"`

Task calling methods
- Delay - `add.delay(2, 2)`
- Async (Queue priority, Countdown after how much time request is made) - `add.apply_async((2, 2), queue='lopri', countdown=10)`
- To add persistency to the tasks recording we add backend to redis-port

Shared tasks - `from celery import shared_task` and functions are enclosed with `@shared_task`

### The broker_url variable name is fixed
`
broker_url = "redis://redis:6379/0"
`
### The result_backend variable name is fixed
`
result_backend = "redis://redis:6379/1"
`
### Add Task priority
`
task_routes = {
    'tasks.math.add': {'priority': 9},
    'tasks.math.mul': {'priority': 0},
}
`
### Task Annotations (like rate limiting)
`
task_annotations = {
    'tasks.math.square': {'rate_limit': '2/m'}
}
`
### Adding workers has two different meaning
- 1. Adding concurrency
- 2. Different worker for different work instances

### To run Schedule (Celery Beat)
- `celery -A celery_app beat --loglevel=info`

### What does celerybeat-schedule stores
- last execution timestamp
- next scheduled run
- task metadata
- schedule hash

### To debug schedule check CLI command for redis
`LRANGE celery 0 -1`

## Concurrency models (how Celery executes tasks)
### A. prefork (default)
- Uses multiple OS processes
- Each process runs independently
`
Master process
   ↓ forks
Worker 1 (process)
Worker 2 (process)
Worker 3 (process)
`
`
Worker Master
 ├── Process 1 → task
 ├── Process 2 → task
 ├── Process 3 → task
 └── Process 4 → task
 `
#### CLI Commands for prefork
`
celery -A celery_app worker --pool=prefork --concurrency=4
`
### B. threads
- Uses Python threads instead of processes
#### CLI Commands for threads
`
celery -A celery_app worker --pool=threads --concurrency=10
`
### C. gevent
- Greenlets (cooperative lightweight threads)
`
celery -A celery_app worker --pool=gevent --concurrency=100
`
### D. eventlet
- Similar to gevent but older ecosystem.
`
celery -A celery_app worker --pool=eventlet --concurrency=100
`

## Playing with worker configurations
```
celery -A celery_app worker \  # starting the worker
  --pool=prefork \             # selecting the concurrency model
  --concurrency=4 \            # Setting value of concurrency
  --autoscale=10,3 \           # Autoscaling worker value (10 is max, 3 is min)
  --prefetch-multiplier=1 \    # reserve task and pass on to workers before execution
  --max-tasks-per-child=100 \  # Number of tasks per worker
  --max-memory-per-child=20000 # Memory limit per worker (here we set it to 20MB)
```
### Here we use message abstraction layer Kombu that sits between Celery and Redis/RabbitMQ
- `from kombu import Queue`
- CodeSnippet for custom queue
```
task_queues = {
    Queue('delayed_task_queue'),
}
```
- CLI command to run custom queue - `celery -A celery_app worker -n worker1@%h -Q delayed_task_queue --loglevel=info -E -n delayed-custom-queue --pool=prefork --concurrency=2 --prefetch-multiplier=2 --max-tasks-per-child=100 --max-memory-per-child=30000` ;
`celery -A celery_app worker -n worker2@%h -Q etl_queue --loglevel=info -E -n etl-custom-queue`
`celery -A celery_app worker -n worker3@%h -Q retry_task_queue --loglevel=info -E -n retry_task_queue`

### Retires
```
except Exception as e:
    raise self.retry(exc=e, countdown=60, max_retries=3)
```
```
@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 10}, retry_backoff=True)
```

### Logging
```
import logging
logger = logging.getLogger(__name__)
logger.info("This is an info log from the log_task.")
logger.warning("This is a warning log from the log_task.")
logger.error("This is an error log from the log_task.")
```