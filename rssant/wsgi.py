"""
WSGI config for rssant project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/howto/deployment/wsgi/
"""

from django.core.wsgi import get_wsgi_application as _get_app

import backdoor
import rssant_common.django_setup  # noqa:F401
from rssant_common.helper import is_main_or_wsgi
from rssant_common.logger import configure_logging
from rssant_config import CONFIG
from rssant_worker.worker_service import WORKER_SERVICE


def get_wsgi_application():
    configure_logging(level=CONFIG.log_level)
    backdoor.setup()
    if CONFIG.is_role_worker:
        WORKER_SERVICE.start_dns_refresh_thread()
    return _get_app()


if is_main_or_wsgi(__name__):
    application = get_wsgi_application()
