import os

from aiohttp.web import json_response
from aiohttp.web_request import Request
from validr import T

from rssant_common import timezone
from rssant_common.image_token import ImageToken, ImageTokenDecodeError
from rssant_config import CONFIG

from .image_proxy import image_proxy
from .rest_validr import ValidrRouteTableDef

routes = ValidrRouteTableDef()


@routes.get('/image/proxy')
async def image_proxy_view_v2(
    request: Request,
    token: T.str,
    url: T.url.maxlen(4096),
):
    try:
        image_token = ImageToken.decode(
            token, secret=CONFIG.image_token_secret, expires=CONFIG.image_token_expires
        )
    except ImageTokenDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    response = await image_proxy(request, url, image_token.referrer)
    return response


UPTIME_BEGIN = timezone.now()


def _get_uptime(now: timezone.datetime):
    uptime_seconds = round((now - UPTIME_BEGIN).total_seconds())
    uptime = str(timezone.timedelta(seconds=uptime_seconds))
    return uptime


def _get_health():
    build_id = os.getenv("EZFAAS_BUILD_ID")
    commit_id = os.getenv("EZFAAS_COMMIT_ID")
    now = timezone.now()
    uptime = _get_uptime(now)
    result = dict(
        build_id=build_id,
        commit_id=commit_id,
        now=now.isoformat(),
        uptime=uptime,
        pid=os.getpid(),
    )
    return result


@routes.get('/image/_health')
async def get_health(request):
    info = _get_health()
    return json_response(info)
