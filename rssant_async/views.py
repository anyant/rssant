import os
from validr import T
from aiohttp.web import json_response
from aiohttp.web_request import Request

from rssant_common import timezone
from rssant_common.image_token import ImageToken, ImageTokenDecodeError
from rssant_config import CONFIG

from .rest_validr import ValidrRouteTableDef
from .image_proxy import image_proxy


routes = ValidrRouteTableDef()


@routes.get('/image/proxy')
async def image_proxy_view_v2(
    request: Request,
    token: T.str,
    url: T.url.maxlen(4096),
):
    try:
        image_token = ImageToken.decode(
            token, secret=CONFIG.image_token_secret,
            expires=CONFIG.image_token_expires)
    except ImageTokenDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    response = await image_proxy(request, url, image_token.referrer)
    return response


@routes.get('/image/_health')
async def get_health(request):
    build_id = os.getenv('RSSANT_BUILD_ID')
    commit_id = os.getenv('RSSANT_COMMIT_ID')
    now = timezone.now().isoformat()
    info = dict(
        build_id=build_id,
        commit_id=commit_id,
        now=now,
    )
    return json_response(info)
