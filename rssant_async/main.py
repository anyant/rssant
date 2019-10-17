from aiohttp import web
from aiojobs.aiohttp import setup as setup_aiojobs
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
import backdoor

from rssant_config import CONFIG
from rssant_common.logger import configure_logging
from rssant_common.helper import is_main_or_wsgi

from .views import routes


def create_app():
    configure_logging(level=CONFIG.log_level)
    backdoor.setup()
    if CONFIG.sentry_enable:
        sentry_sdk.init(
            dsn=CONFIG.sentry_dsn,
            integrations=[AioHttpIntegration()]
        )
    api = web.Application()
    api.router.add_routes(routes)
    app = web.Application()
    app.add_subapp('/api/v1', api)
    setup_aiojobs(app, limit=5000, pending_limit=5000)
    return app


if is_main_or_wsgi(__name__):
    app = create_app()


if __name__ == "__main__":
    web.run_app(app, port=6786)
