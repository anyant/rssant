import os

import click
import gunicorn.app.base

from .app import create_app


class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        for key, value in self.options.items():
            if key not in self.cfg.settings:
                raise ValueError(f'Unknown gunicorn option {key!r}')
            self.cfg.set(key, value)

    def load(self):
        return self.application


@click.command()
def main():
    """Run rssant asyncapi server."""
    bind = os.getenv('RSSANT_BIND_ADDRESS') or '0.0.0.0:6786'
    workers = int(os.getenv('RSSANT_NUM_WORKERS') or 1)
    keep_alive = int(os.getenv('RSSANT_KEEP_ALIVE') or 2)
    options = {
        'bind': bind,
        'workers': workers,
        'keepalive': keep_alive,
        'worker_class': 'aiohttp.GunicornWebWorker',
        'forwarded_allow_ips': '*',
        'reuse_port': True,
        'timeout': 300,
        'accesslog': '-',
        'errorlog': '-',
        'loglevel': 'info',
    }
    wsgi_app = create_app()
    server = StandaloneApplication(wsgi_app, options)
    server.run()


if __name__ == "__main__":
    main()
