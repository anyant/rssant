import logging

import click

from rssant_common.logger import configure_logging
from rssant_config import CONFIG

from .scheduler import RssantScheduler

LOG = logging.getLogger(__name__)


@click.command()
def main():
    """Run rssant scheduler."""
    configure_logging(level=CONFIG.log_level)
    scheduler = RssantScheduler(num_worker=CONFIG.scheduler_num_worker)
    scheduler.main()


if __name__ == '__main__':
    main()
