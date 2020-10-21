from validr import T
from aiohttp.web import json_response

from rssant_common.image_url import decode_image_url, ImageUrlDecodeError
from rssant_common.image_token import ImageToken, ImageTokenDecodeError
from rssant_config import CONFIG
from .rest_validr import ValidrRouteTableDef
from .image_proxy import image_proxy


routes = ValidrRouteTableDef()


@routes.get('/image/proxy')
async def image_proxy_view_v2(request, token: T.str, url: T.url):
    try:
        token = ImageToken.decode(
            token, secret=CONFIG.image_token_secret,
            expires=CONFIG.image_token_expires)
    except ImageTokenDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    if not url.startswith(token.url_root):
        return json_response({'message': 'url and token not match'}, status=400)
    response = await image_proxy(request, url, token.referrer)
    return response


@routes.get('/image/{image}')
async def proxy_story_image(request, image: T.str):
    try:
        image_url = decode_image_url(image)
    except ImageUrlDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    response = await image_proxy(request, image_url['url'], image_url['referer'])
    return response
