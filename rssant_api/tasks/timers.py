from celery import shared_task as task


@task
def add(a, b):
    return a + b
