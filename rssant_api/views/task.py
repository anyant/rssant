from validr import T
from django_rest_validr import RestRouter

from rssant_api.tasks.timers import add

TaskView = RestRouter()


@TaskView.post('task/add')
def task_create(request, a: T.int, b: T.int) -> T.dict(message=T.str):
    result = add.delay(a, b)
    return dict(message=repr(result))
