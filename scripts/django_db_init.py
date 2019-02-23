#!/usr/bin/env python
import os
import sys
sys.path.insert(0, os.getcwd())
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')

import django  # noqa: E402
django.setup()

from allauth.socialaccount.models import SocialApp  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.contrib.sites.models import Site  # noqa: E402
from django.conf import settings  # noqa: E402
from allauth.socialaccount.providers.github.provider import GitHubProvider  # noqa: E402


User = get_user_model()


def main():
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


if __name__ == "__main__":
    main()
