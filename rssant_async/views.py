from validr import T
from aiohttp.web import json_response

from rssant_common.image_url import decode_image_url, ImageUrlDecodeError
from .rest_validr import ValidrRouteTableDef
from .image_proxy import image_proxy


routes = ValidrRouteTableDef()


@routes.get('/image/{image}')
async def proxy_story_image(request, image: T.str):
    try:
        image_url = decode_image_url(image)
    except ImageUrlDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    response = await image_proxy(request, image_url['url'], image_url['referer'])
    return response
