import os

import django
from aiohttp import web
from aiojobs.aiohttp import setup as setup_aiojobs
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
import backdoor

from rssant.settings import ENV_CONFIG
from rssant_common.logger import configure_logging

from .views import routes
from .callback_client import CallbackClient
from .redis_dao import REDIS_DAO


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')


def main():
    configure_logging()
    django.setup()

    if ENV_CONFIG.sentry_enable:
        sentry_sdk.init(
            dsn=ENV_CONFIG.sentry_dsn,
            integrations=[AioHttpIntegration()]
        )

    api = web.Application()
    api.router.add_routes(routes)
    app = web.Application()
    app.add_subapp('/api/v1', api)
    setup_aiojobs(app, limit=5000, pending_limit=5000)
    app.on_cleanup.append(lambda app: CallbackClient.close())
    app.on_startup.append(lambda app: REDIS_DAO.init())
    app.on_cleanup.append(lambda app: REDIS_DAO.close())

    backdoor.setup()
    web.run_app(app, port=6786)


if __name__ == "__main__":
    main()
