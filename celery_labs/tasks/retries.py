from celery import shared_task

@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 10}, retry_backoff=True)
def retry_task(self):
    try:
        # Simulation of a task failing
        import random
        if random.random() < 0.5:  # 50% chance of failure
            raise Exception("Simulated task failure")
        return "Task succeeded"
    except Exception as e:
        raise self.retry(exc=e, countdown=10, max_retries=5)