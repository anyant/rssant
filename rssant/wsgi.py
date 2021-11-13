"""
WSGI config for rssant project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/howto/deployment/wsgi/
"""
import backdoor
from django.core.wsgi import get_wsgi_application as _get_app
import rssant_common.django_setup  # noqa:F401
from rssant_config import CONFIG
from rssant_common.logger import configure_logging
from rssant_common.helper import is_main_or_wsgi


def get_wsgi_application():
    configure_logging(level=CONFIG.log_level)
    backdoor.setup()
    return _get_app()


if is_main_or_wsgi(__name__):
    application = get_wsgi_application()
