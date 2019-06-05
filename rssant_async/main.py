import os
import logging

from aiohttp import web
from aiojobs.aiohttp import setup as setup_aiojobs

import rssant.settings  # noqa
from .views import routes
from .callback_client import CallbackClient

# You must either define the environment variable DJANGO_SETTINGS_MODULE or
# call settings.configure() before accessing settings.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')

LOG_FORMAT = "%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

api = web.Application()
api.router.add_routes(routes)
app = web.Application()
app.add_subapp('/api/v1', api)
setup_aiojobs(app, limit=5000, pending_limit=5000)
app.on_cleanup.append(CallbackClient.close)


if __name__ == "__main__":
    web.run_app(app, port=6786)
