from celery_app import app

@app.task
def square(x):
    return x * x