"""
https://stackoverflow.com/questions/39704298/how-to-call-django-setup-in-console-script
"""
import os
import django
from django.apps import apps
from django.conf import settings


def _django_setup():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
    if not apps.ready and not settings.configured:
        django.setup()


_django_setup()
