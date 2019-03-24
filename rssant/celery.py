import os
from celery import Celery
from celery.utils.log import get_task_logger
from rssant.sentry import setup_sentry_celery

LOG = get_task_logger(__name__)

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')

app = Celery('rssant')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

setup_sentry_celery()

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.task(bind=True, name='rssant.tasks.debug')
def debug_task(self):
    LOG.info('Request: {0!r}'.format(self.request))
