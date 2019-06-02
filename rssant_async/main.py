import logging

from aiohttp import web
from aiojobs.aiohttp import setup as setup_aiojobs

from .views import routes
from .callback_client import CallbackClient

LOG_FORMAT = "%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

api = web.Application()
api.router.add_routes(routes)
app = web.Application()
app.add_subapp('/api/v1', api)
setup_aiojobs(app)
app.on_cleanup.append(CallbackClient.close)


if __name__ == "__main__":
    web.run_app(app)
