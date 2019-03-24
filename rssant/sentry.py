import os

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration


def setup_sentry(sentry_dsn):
    if sentry_dsn:
        print('Sentry enabled, PID={}!'.format(os.getpid()))
    integrations = [DjangoIntegration(), CeleryIntegration()]
    sentry_sdk.init(sentry_dsn, integrations=integrations)
