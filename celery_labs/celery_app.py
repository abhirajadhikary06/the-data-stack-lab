from celery import Celery
app = Celery('task', 
            include=['tasks.math', 'tasks.heartbeat', 'tasks.etl'])
app.config_from_object('celeryconfig')