from celery import shared_task
import logging
from observability.metrics import log_message_length, log_messages_total

logger = logging.getLogger(__name__)

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3} )
def log_task(self):
    try:
        info_msg = "This is an info log from the log_task."
        warning_msg = "This is a warning log from the log_task."
        error_msg = "This is an error log from the log_task."

        logger.info(info_msg)
        logger.warning(warning_msg)
        logger.error(error_msg)

        task_name = "tasks.logger.log_task"
        for level, message in (("info", info_msg), ("warning", warning_msg), ("error", error_msg)):
            log_messages_total.labels(task_name=task_name, log_level=level).inc()
            log_message_length.labels(task_name=task_name, log_level=level).observe(len(message))

        return "Logs have been recorded successfully."
    except Exception as e:
        log_messages_total.labels(task_name="tasks.logger.log_task", log_level="exception").inc()
        logger.exception("An error occurred in log_task.")
        raise self.retry(exc=e)