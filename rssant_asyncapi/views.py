from aiohttp.web import json_response
from aiohttp.web_request import Request
from validr import T

from rssant_common.health import health_info
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


@routes.get('/image/_health')
async def get_health(request):
    result = health_info()
    result.update(role='asyncapi')
    return json_response(result)
