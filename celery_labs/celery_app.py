from celery import Celery
app = Celery('task', 
            include=['tasks.math', 'tasks.heartbeat', 'tasks.etl', 'tasks.delay',
                     'tasks.retries', 'tasks.logger'])
app.config_from_object('celeryconfig')