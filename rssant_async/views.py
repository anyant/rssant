from validr import T
from aiohttp.web import json_response
from aiohttp.web_request import Request

from rssant_common.image_token import ImageToken, ImageTokenDecodeError
from rssant_config import CONFIG
from .rest_validr import ValidrRouteTableDef
from .image_proxy import image_proxy


routes = ValidrRouteTableDef()

IMAGE_OWNER_COOKIE = 'rssant-image-owner'


@routes.get('/image/proxy')
async def image_proxy_view_v2(request: Request, token: T.str, url: T.url):
    try:
        image_token = ImageToken.decode(
            token, secret=CONFIG.image_token_secret,
            expires=CONFIG.image_token_expires)
    except ImageTokenDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    owner = request.cookies.get(IMAGE_OWNER_COOKIE)
    if (not image_token.owner) or image_token.owner != owner:
        err_msg = '403 Forbidden Access / not owner'
        return json_response({'message': err_msg}, status=403)
    response = await image_proxy(request, url, image_token.referrer)
    return response


@routes.get('/image/active-proxy')
async def image_proxy_active(request, user_id: T.str.maxlen(32)):
    response = json_response({'message': 'OK'})
    response.set_cookie(
        IMAGE_OWNER_COOKIE,
        user_id,
        path='/api/v1/image',
        httponly=True,
    )
    return response
