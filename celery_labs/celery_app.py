import time
from celery import Celery, signals

from observability.metrics import (
    active_workers,
    retry_attempts_total,
    tasks_duration_seconds,
    tasks_failed_total,
    tasks_last_run_timestamp,
    tasks_total,
)

app = Celery('task', 
            include=['tasks.math', 'tasks.heartbeat', 'tasks.etl', 'tasks.delay',
                     'tasks.retries', 'tasks.logger'])
app.config_from_object('celeryconfig')

_TASK_START_TIMES = {}


def _task_name(sender=None, task=None):
    if sender is not None and getattr(sender, "name", None):
        return sender.name
    if task is not None and getattr(task, "name", None):
        return task.name
    return "unknown_task"


def _worker_name(task=None):
    request = getattr(task, "request", None)
    hostname = getattr(request, "hostname", None)
    return hostname or "unknown_worker"

# SIGNALS
@signals.task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, **kwargs):
    task_name = _task_name(sender=sender, task=task)
    worker_name = _worker_name(task=task)
    _TASK_START_TIMES[task_id] = time.perf_counter()
    tasks_last_run_timestamp.labels(task_name=task_name).set(time.time())
    active_workers.labels(worker_name=worker_name).inc()

@signals.task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, retval=None, state=None, **kwargs):
    task_name = _task_name(sender=sender, task=task)
    worker_name = _worker_name(task=task)
    start = _TASK_START_TIMES.pop(task_id, None)

    if start is not None:
        tasks_duration_seconds.labels(task_name=task_name).observe(time.perf_counter() - start)

    if state == "SUCCESS":
        tasks_total.labels(task_name=task_name).inc()

    active_workers.labels(worker_name=worker_name).dec()

@signals.task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, **kwargs):
    task_name = _task_name(sender=sender)
    tasks_failed_total.labels(task_name=task_name).inc()

@signals.task_retry.connect
def task_retry_handler(sender=None, task_id=None, task=None, **kwargs):
    task_name = _task_name(sender=sender, task=task)
    retry_attempts_total.labels(task_name=task_name).inc()