import json

from aiohttp import web

from rssant_common.health import health_info

routes = web.RouteTableDef()


def JsonResponse(data: dict):
    text = json.dumps(data, ensure_ascii=False, indent=4)
    return web.json_response(text=text)


@routes.get('/')
async def index(request):
    return JsonResponse({'message': '你好，RSSAnt Async API！'})


@routes.get('/health')
async def health(request):
    result = health_info()
    result.update(role='scheduler')
    return JsonResponse(result)
