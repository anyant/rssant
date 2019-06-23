from validr import T
from aiohttp.web import json_response
from aiojobs.aiohttp import spawn

from rssant_common.image_url import decode_image_url, ImageUrlDecodeError
from .rest_validr import ValidrRouteTableDef
from .tasks import fetch_story, detect_story_images
from .redis_dao import REDIS_DAO
from .image_proxy import image_proxy


routes = ValidrRouteTableDef()


@routes.post('/async/fetch_storys')
async def api_fetch_storys(
    request,
    storys: T.list(T.dict(id = T.str, url = T.url)).unique,
    callback: T.str.optional,
) -> T.dict(message=T.str):
    for s in storys:
        await spawn(request, fetch_story(s['id'], s['url'], callback))
    return {'message': 'OK'}


@routes.get('/async/get_story')
async def api_get_story(
    request,
    id: T.str,
) -> T.dict(
    id=T.str,
    url=T.url,
    status=T.int,
    encoding=T.str.optional,
    text=T.str.maxlen(10 * 1024 * 1024).optional
):
    story = await REDIS_DAO.get_story(id)
    if not story:
        return json_response({'message': 'Not Found'}, status=400)
    return story


@routes.post('/async/detect_story_images')
async def api_detect_story_images(
    request,
    story: T.dict(id = T.str, url = T.url.optional),
    images: T.list(T.dict(url = T.url)).unique,
    callback: T.str.optional,
) -> T.dict(message=T.str):
    images_urls = [x['url'] for x in images]
    await spawn(request, detect_story_images(story['id'], story['url'], images_urls, callback))
    return {'message': 'OK'}


@routes.get('/image/{image}')
async def proxy_story_image(request, image: T.str):
    try:
        image_url = decode_image_url(image)
    except ImageUrlDecodeError as ex:
        return json_response({'message': str(ex)}, status=400)
    response = await image_proxy(request, image_url['url'], image_url['referer'])
    return response
