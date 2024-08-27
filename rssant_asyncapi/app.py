from aiohttp import web

from rssant_config import CONFIG
from rssant_common.logger import configure_logging

from .views import routes


def create_app():
    configure_logging(level=CONFIG.log_level)
    api = web.Application()
    api.router.add_routes(routes)
    app = web.Application()
    app.add_subapp('/api/v1', api)
    return app
