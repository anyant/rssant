import logging
import os

from django.db import connection as CONNECTION
from django.http import JsonResponse as _JsonResponse
from django.urls import path

from rssant_common import timezone
from rssant_common.network_helper import LOCAL_IP_LIST
from rssant_config import CONFIG

LOG = logging.getLogger(__name__)


def JsonResponse(data: dict):
    return _JsonResponse(data, json_dumps_params={'ensure_ascii': False})


def index(request):
    return JsonResponse({'message': "你好, RSSAnt!"})


def error(request):
    raise ValueError(request.GET.get('error') or 'A value error!')


UPTIME_BEGIN = timezone.now()


def _get_uptime(now: timezone.datetime):
    uptime_seconds = round((now - UPTIME_BEGIN).total_seconds())
    uptime = str(timezone.timedelta(seconds=uptime_seconds))
    return uptime


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
    build_id = os.getenv("EZFAAS_BUILD_ID")
    commit_id = os.getenv("EZFAAS_COMMIT_ID")
    now = timezone.now()
    uptime = _get_uptime(now)
    ip_list = [ip for _, ip in LOCAL_IP_LIST]
    result = dict(
        role=CONFIG.role,
        build_id=build_id,
        commit_id=commit_id,
        now=now.isoformat(),
        uptime=uptime,
        pid=os.getpid(),
        ip_list=ip_list,
    )
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
