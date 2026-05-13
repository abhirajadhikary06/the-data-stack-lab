from datetime import timedelta
from celery.schedules import crontab
from kombu import Queue

broker_url = "redis://redis:6379/0"
result_backend = "redis://redis:6379/1"
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Kolkata'
enable_utc = True

# CUSTOM AND DEFAULT QUEUES
task_queues = {
    Queue('delayed_task_queue'),
    Queue('etl_queue'),
    Queue('default'),
}

# ROUTING AND PRIORITY
task_routes = {
    'tasks.math.add': {'priority': 9},
    'tasks.math.mul': {'priority': 0},
    'tasks.math.square': {'priority': 5},
    'tasks.etl.*': {'priority': 7},
    'tasks.delay.*': {'queue': 'delayed_task_queue', 'priority': 6},
    'tasks.etl.*': {'queue': 'etl_queue', 'priority': 7},
}

# TASK ANNOTATIONS
task_annotations = {
    'tasks.math.square': {'rate_limit': '2/m'},
    'tasks.delay.delay_task': {'rate_limit': '10/m'},
}

# BEAT SCHEDULE
beat_schedule = {
    "heartbeat":{
        "task": "tasks.heartbeat.heartbeat_task",
        "schedule": timedelta(seconds=100),
    },
    "cron_task":{
        "task": "tasks.heartbeat.cron_task",
        "schedule": crontab(minute="*/30"),
    },
# daily-etl-pipeline
    "daily_etl_pipeline": {
        "task": "tasks.etl.daily_etl_pipeline",
        "schedule": crontab(minute=5),  # Every 5 minutes for testing
    },
}