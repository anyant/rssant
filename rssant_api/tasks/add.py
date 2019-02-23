from celery import shared_task as task


@task(name='rssant.tasks.add')
def add(x, y):
    return x + y
