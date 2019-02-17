from validr import T
from django_rest_validr import RestRouter


TaskView = RestRouter()


@TaskView.post('task/add')
def task_create(request, a: T.int, b: T.int) -> T.dict(message=T.str):
    return dict(message=str(a + b))
