import os

from rssant_common import timezone
from rssant_common.network_helper import LOCAL_IP_LIST

UPTIME_BEGIN = timezone.now()


def _get_uptime(now: timezone.datetime):
    uptime_seconds = round((now - UPTIME_BEGIN).total_seconds())
    uptime = str(timezone.timedelta(seconds=uptime_seconds))
    return uptime


def health_info():
    build_id = os.getenv("EZFAAS_BUILD_ID")
    commit_id = os.getenv("EZFAAS_COMMIT_ID")
    now = timezone.now()
    uptime = _get_uptime(now)
    ip_list = [ip for _, ip in LOCAL_IP_LIST]
    result = dict(
        build_id=build_id,
        commit_id=commit_id,
        now=now.isoformat(),
        uptime=uptime,
        pid=os.getpid(),
        ip_list=ip_list,
    )
    return result
