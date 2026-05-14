from celery import shared_task
import logging
logger = logging.getLogger(__name__)

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3} )
def log_task(self):
    try:
        logger.info("This is an info log from the log_task.")
        logger.warning("This is a warning log from the log_task.")
        logger.error("This is an error log from the log_task.")
        return "Logs have been recorded successfully."
    except Exception as e:
        logger.exception("An error occurred in log_task.")
        raise self.retry(exc=e)