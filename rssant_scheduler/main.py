import logging

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


@click.command()
@click.option('--host', type=str, default='0.0.0.0')
@click.option('--port', type=int, default=6790)
def main(host: str, port: int):
    """Run rssant scheduler."""
    configure_logging(level=CONFIG.log_level)
    scheduler = RssantScheduler(num_worker=CONFIG.scheduler_num_worker)
    scheduler.start()
    app = create_app()
    web.run_app(app, host=host, port=port, reuse_port=True)


if __name__ == "__main__":
    main()
