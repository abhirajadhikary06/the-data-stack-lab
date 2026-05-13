from datetime import timedelta
from celery.schedules import crontab

broker_url = "redis://redis:6379/0"
result_backend = "redis://redis:6379/1"
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Kolkata'
enable_utc = True

task_routes = {
    'tasks.math.add': {'priority': 9},
    'tasks.math.mul': {'priority': 0},
    'tasks.math.square': {'priority': 5},
    'tasks.etl.*': {'priority': 7},
}

task_annotations = {
    'tasks.math.square': {'rate_limit': '2/m'},
    'tasks.delay.delay_task': {'rate_limit': '10/m'},
}

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
        "schedule": crontab(minute=0, hour="*/6"),  # Every 6 hours
    },
}

