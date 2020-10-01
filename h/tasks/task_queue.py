from h.celery import celery


@celery.task
def eat():
    celery.request.find_service(name="task_queue").eat()
