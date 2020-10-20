import click
from rssant_common.logger import configure_logging
from .cacert import CacertHelper


@click.group()
def cli():
    """cacert commands"""


@click.option('--debug', is_flag=True)
@cli.command()
def update(debug):
    """update cacert"""
    if debug:
        configure_logging(level='DEBUG')
    CacertHelper.update()
