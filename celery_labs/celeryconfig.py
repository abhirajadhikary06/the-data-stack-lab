from datetime import timedelta
from celery.schedules import crontab
from kombu import Queue

broker_url = "amqp://username:password@rabbitmq:5672//"
result_backend = "redis://redis:6379/1"

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]

timezone = "Asia/Kolkata"
enable_utc = True

task_track_started = True

broker_connection_retry_on_startup = True

task_default_queue = "default"

task_queue_max_priority = 10
task_default_priority = 5

# QUEUES
task_queues = (
    Queue("default"),
    Queue("math_task_queue"),
    Queue("delayed_task_queue"),
    Queue("etl_queue"),
    Queue("retry_task_queue"),
    Queue("log_task_queue"),
    Queue("migration_neon_monet"),
    Queue("dask_transform")
)

# ROUTES
task_routes = {
    "tasks.math.*": {
        "queue": "math_task_queue",
        "priority": 5,
    },

    "tasks.delay.*": {
        "queue": "delayed_task_queue",
        "priority": 6,
    },

    "tasks.etl.*": {
        "queue": "etl_queue",
        "priority": 7,
    },

    "tasks.retries.*": {
        "queue": "retry_task_queue",
        "priority": 8,
    },

    "tasks.logger.*": {
        "queue": "log_task_queue",
        "priority": 4,
    },
    "telecom_etl.migration.*": {
        "queue": "migration_neon_monet",
        "priority": 9,
    },
    "telecom_etl.transform.*": {
        "queue": "dask_transform",
        "priority": 10,
    }
}

# ANNOTATIONS
task_annotations = {
    "tasks.math.square": {"rate_limit": "2/m"},
    "tasks.delay.delay_task": {"rate_limit": "10/m"},
    "tasks.etl.daily_etl_pipeline": {"rate_limit": "1/m"},
    "tasks.retries.retry_task": {"rate_limit": "100/m"},
    "tasks.logger.log_task": {"rate_limit": "1000/m"},
}

# BEAT
beat_schedule = {
    "heartbeat": {
        "task": "tasks.heartbeat.heartbeat_task",
        "schedule": timedelta(minutes=30),
    },

    "cron_task": {
        "task": "tasks.heartbeat.cron_task",
        "schedule": crontab(minute="*/30"),
    },

    "daily_etl_pipeline": {
        "task": "tasks.etl.daily_etl_pipeline",
        "schedule": crontab(minute="*/5"),
    },

    "retries_task": {
        "task": "tasks.retries.retry_task",
        "schedule": crontab(minute="*/5"),
    },

    "log_task": {
        "task": "tasks.logger.log_task",
        "schedule": crontab(minute="*"),
    },
}