from validr import T

from .rest_validr import ValidrRouteTableDef


routes = ValidrRouteTableDef()


@routes.get('/hello')
async def fetch_storys(
    request,
    name: T.str.default('world'),
) -> T.dict(hello=T.str):
    return {'hello': name}


async def detect_story_images(request,):
    pass
