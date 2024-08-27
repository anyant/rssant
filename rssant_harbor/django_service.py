import logging
import time
from importlib import import_module
from urllib.parse import urlparse

import django.db
from allauth.socialaccount.models import SocialApp
from allauth.socialaccount.providers.github.provider import GitHubProvider
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.sites.models import Site
from django.db import connection

LOG = logging.getLogger(__name__)


def django_clear_expired_sessions():
    """
    see also: django/contrib/sessions/management/commands/clearsessions.py
    """
    begin_time = time.time()
    engine = import_module(settings.SESSION_ENGINE)
    try:
        engine.SessionStore.clear_expired()
    except NotImplementedError:
        msg = (
            "Session engine '%s' doesn't support clearing "
            "expired sessions." % settings.SESSION_ENGINE
        )
        LOG.info(msg)
    else:
        cost = time.time() - begin_time
        LOG.info('django_clear_expired_sessions cost {:.1f}ms'.format(cost * 1000))


def django_run_db_init():
    # create postgres extension
    with connection.cursor() as cursor:
        try:
            cursor.execute('create extension hstore;')
        except django.db.ProgrammingError as ex:
            if 'already exists' not in str(ex):
                raise
            LOG.info(str(ex).strip())
        else:
            LOG.info('extension hstore created')
    # create superuser
    User = get_user_model()
    admin_email = settings.ENV_CONFIG.admin_email
    try:
        admin = User.objects.get(username='admin')
    except User.DoesNotExist:
        LOG.info('create admin user')
        User.objects.create_superuser('admin', admin_email, 'admin')
    else:
        LOG.info('update admin user email')
        admin.email = admin_email
        admin.save()
    # update site
    root_url = urlparse(settings.ENV_CONFIG.root_url)
    LOG.info('update site info')
    site = Site.objects.get_current()
    site.domain = root_url.netloc
    site.name = root_url.netloc
    site.save()
    # create social app
    client_id = settings.SOCIAL_APP_GITHUB['client_id']
    secret = settings.SOCIAL_APP_GITHUB['secret']
    provider = GitHubProvider.id
    try:
        social_app = SocialApp.objects.get(provider=provider, name='github')
    except SocialApp.DoesNotExist:
        LOG.info('create github social app')
        social_app = SocialApp(
            provider=provider, name='github', client_id=client_id, secret=secret
        )
    else:
        LOG.info('update github social app')
        social_app.client_id = client_id
        social_app.secret = secret
    social_app.save()
    if not social_app.sites.filter(pk=site.pk).exists():
        social_app.sites.add(site)
        social_app.save()
