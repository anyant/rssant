from itertools import chain

from validr import T
from celery.task.control import inspect as celery_inspect

from django_rest_validr import RestRouter
from rssant.celery import app as celery_app

TaskView = RestRouter()


@TaskView.get('task/')
def task_list(
    request,
) -> T.list(T.str):
    tasks = set(chain.from_iterable(celery_inspect().registered_tasks().values()))
    return tasks


@TaskView.post('task/<str:name>/')
def task_create(
    request,
    name: T.str,
    params: T.dict.optional,
) -> T.dict(task_id=T.str):
    if params is None:
        params = {}
    async_result = celery_app.send_task(name, kwargs=params)
    return dict(task_id=str(async_result.id))
