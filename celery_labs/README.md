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