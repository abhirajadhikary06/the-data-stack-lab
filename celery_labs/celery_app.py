from celery import Celery
app = Celery('task', broker='redis://redis:6379/0', backend='redis://redis:6379/1')

app.autodiscover_tasks(['tasks'])

@app.task
def add(x, y):
    return x + y

@app.task
def mul(x,y):
    return x * y