import logging
import os

import click
from aiohttp import web

from rssant_common.logger import configure_logging
from rssant_config import CONFIG

from .scheduler import RssantScheduler
from .views import routes

LOG = logging.getLogger(__name__)


def create_app():
    api = web.Application()
    api.add_routes(routes)
    app = web.Application()
    app.add_subapp('/api/v1', api)
    return app


def _get_env_bind_address() -> tuple:
    bind = os.getenv('RSSANT_BIND_ADDRESS') or '0.0.0.0:6790'
    host, port = bind.split(':')
    port = int(port)
    return host, port


@click.command()
def main():
    """Run rssant scheduler."""
    configure_logging(level=CONFIG.log_level)
    scheduler = RssantScheduler(num_worker=CONFIG.scheduler_num_worker)
    scheduler.start()
    app = create_app()
    host, port = _get_env_bind_address()
    web.run_app(app, host=host, port=port, reuse_port=True)


if __name__ == "__main__":
    main()
