"""
https://stackoverflow.com/questions/39704298/how-to-call-django-setup-in-console-script
"""
import os
from pathlib import Path
import django
from django.apps import apps
from django.conf import settings
from django.utils.autoreload import autoreload_started


_root_dir = Path(__file__).parent.parent


def _watch_changelog(sender, **kwargs):
    sender.watch_dir(_root_dir / 'docs/changelog', '*')
    sender.watch_dir(_root_dir / 'rssant_common/resources', '*')


def _django_setup():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
    if not apps.ready and not settings.configured:
        django.setup()
        autoreload_started.connect(_watch_changelog)


_django_setup()
