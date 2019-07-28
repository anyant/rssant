"""
WSGI config for rssant project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application
from rssant_common.logger import configure_logging

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')

configure_logging()
application = get_wsgi_application()
