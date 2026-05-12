First need to have a python file with Celery initialized and python function enclosed under `@app.task` decorator
Docker Shell - `docker exec -it celery_worker bash`
Run Celery Worker - `celery -A celery_app worker --loglevel=info -E`
Test Run the task created in celery_app.py - `docker exec celery_worker python -c "from celery_app import add; print(add.delay(10, 20))"`

- To add persistency to the tasks recording we add backend to redis-port