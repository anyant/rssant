import logging

from django.db import connection as CONNECTION
from django.http import JsonResponse as _JsonResponse
from django.urls import path

from rssant_common.health import health_info
from rssant_config import CONFIG

LOG = logging.getLogger(__name__)


def JsonResponse(data: dict):
    params = {'ensure_ascii': False, 'indent': 4}
    return _JsonResponse(data, json_dumps_params=params)


def index(request):
    return JsonResponse({'message': "你好, RSSAnt!"})


def error(request):
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


def _get_health():
    result = health_info()
    result.update(role=CONFIG.role)
    if not CONFIG.is_role_worker:
        is_db_ok = _check_db_health()
        result.update(is_db_ok=is_db_ok)
    return result


def health(request):
    return JsonResponse(_get_health())


urls = [
    path('', index),
    path('error', error),
    path('health', health),
]
