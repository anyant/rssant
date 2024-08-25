import logging

from django.db import connection as CONNECTION
from django.http import HttpRequest
from django.http import JsonResponse as _JsonResponse
from django.urls import path

from rssant_api.models import WorkerTask
from rssant_common.health import health_info
from rssant_config import CONFIG

LOG = logging.getLogger(__name__)


def JsonResponse(data: dict):
    params = {'ensure_ascii': False, 'indent': 4}
    return _JsonResponse(data, json_dumps_params=params)


def on_index(request):
    return JsonResponse({'message': "你好, RSSAnt!"})


def on_error(request):
    raise ValueError(request.GET.get('error') or 'A value error!')


def _check_db_health():
    try:
        with CONNECTION.cursor() as db:
            db.execute('SELECT 1')
    except Exception as ex:
        LOG.info('check_db_health %s', ex, exc_info=True)
        connected = False
    else:
        connected = True
    return connected


def _check_task_stats():
    task_stats = WorkerTask.stats()
    return task_stats


def _get_health():
    result = health_info()
    result.update(role=CONFIG.role)
    if CONFIG.is_role_api:
        is_db_ok = _check_db_health()
        result.update(is_db_ok=is_db_ok)
        if is_db_ok:
            task_stats = _check_task_stats()
            result.update(task_stats=task_stats)
    return result


def on_health(request):
    return JsonResponse(_get_health())


def _to_json(v):
    if v is None or isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


def on_debug(request: HttpRequest):
    headers = dict(request.headers)
    meta = {k: _to_json(v) for k, v in request.META.items()}
    url = request.get_raw_uri()
    result = dict(
        url=url,
        method=request.method,
        path=request.path,
        headers=headers,
        meta=meta,
    )
    return JsonResponse(result)


urls = [
    path('', on_index),
    path('error', on_error),
    path('health', on_health),
    path('debug', on_debug),
]
