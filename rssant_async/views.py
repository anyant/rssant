from validr import T
from aiohttp.web import json_response
from aiojobs.aiohttp import spawn

from .rest_validr import ValidrRouteTableDef
from .tasks import STORYS_BUFFER, fetch_story, detect_story_images


routes = ValidrRouteTableDef()


@routes.post('/task/fetch_storys')
async def api_fetch_storys(
    request,
    storys: T.list(T.dict(id = T.str, url = T.url)).unique,
    callback: T.url.optional,
) -> T.dict(message=T.str):
    for s in storys:
        await spawn(request, fetch_story(s['id'], s['url'], callback))
    return {'message': 'OK'}


@routes.get('/task/get_story')
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
    story = STORYS_BUFFER.get(id)
    if not story:
        return json_response({'message': 'Not Found'}, status=400)
    return story


@routes.post('/task/detect_story_images')
async def api_detect_story_images(
    request,
    story: T.dict(id = T.str, url = T.url),
    images: T.list(T.dict(url = T.url)).unique,
    callback: T.url.optional,
) -> T.dict(message=T.str):
    images_urls = [x['url'] for x in images]
    await spawn(request, detect_story_images(story['id'], story['url'], images_urls, callback))
    return {'message': 'OK'}


@routes.get('/image/{image}')
async def proxy_story_image(request, image: T.str):
    pass
