import logging

from rssant_harbor.django_service import django_run_db_init

LOG = logging.getLogger(__name__)


def run():
    django_run_db_init()
