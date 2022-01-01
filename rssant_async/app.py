from aiohttp import web
from aiojobs.aiohttp import setup as setup_aiojobs

from rssant_config import CONFIG
from rssant_common.logger import configure_logging

from .views import routes


def create_app():
    configure_logging(level=CONFIG.log_level)
    api = web.Application()
    api.router.add_routes(routes)
    app = web.Application()
    app.add_subapp('/api/v1', api)
    setup_aiojobs(app, limit=5000, pending_limit=5000)
    return app
