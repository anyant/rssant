import logging

import click

import rssant_common.django_setup  # noqa:F401

LOG = logging.getLogger(__name__)


@click.group()
def main():
    """User Commands"""


if __name__ == "__main__":
    main()
