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
@click.option('--bind', type=str, default='0.0.0.0:6786')
@click.option('--workers', type=int, default=1)
@click.option('--keep-alive', type=int, default=2)
def main(bind: str, workers: int, keep_alive: int):
    """Run rssant async server."""
    options = {
        'bind': bind,
        'workers': workers,
        'keepalive': keep_alive,
        'worker_class': 'aiohttp.GunicornWebWorker',
        'forwarded_allow_ips': '*',
        'accesslog': '-',
        'errorlog': '-',
        'loglevel': 'info',
    }
    wsgi_app = create_app()
    server = StandaloneApplication(wsgi_app, options)
    server.run()


if __name__ == "__main__":
    main()
