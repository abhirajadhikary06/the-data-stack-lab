from celery import shared_task
from datetime import datetime

@shared_task
def heartbeat_task():
    current_time = datetime.now()
    print(f"Heartbeat at {current_time}")
    return f"Heartbeat checked at {current_time}"    

@shared_task
def cron_task():
    current_time = datetime.now()
    print(f"Cron task executed at {current_time}")
    return f"Cron task executed at {current_time}"