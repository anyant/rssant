import django.db
from django.db import connection
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.sites.models import Site
from allauth.socialaccount.models import SocialApp
from allauth.socialaccount.providers.github.provider import GitHubProvider


User = get_user_model()


def run():
    # create postgres extension
    with connection.cursor() as cursor:
        try:
            cursor.execute('create extension hstore;')
        except django.db.ProgrammingError:
            pass  # ignore: already exists
    # create superuser
    User.objects.create_superuser('admin', 'admin@anyant.com', 'admin')
    # update site
    site = Site.objects.get_current()
    site.domain = '127.0.0.1:6789'
    site.name = '127.0.0.1:6789'
    site.save()
    # create social app
    client_id = settings.SOCIAL_APP_GITHUB['client_id']
    secret = settings.SOCIAL_APP_GITHUB['secret']
    social_app = SocialApp(
        provider=GitHubProvider.id, name='github',
        client_id=client_id, secret=secret)
    social_app.save()
    social_app.sites.add(site)
    social_app.save()
