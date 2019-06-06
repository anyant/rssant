import os
import logging

from raven import Client
from raven.contrib.celery import register_signal, register_logger_signal


def setup_sentry_celery():
    # https://docs.sentry.io/clients/python/integrations/celery/
    # https://github.com/getsentry/raven-python/issues/1189
    # https://github.com/getsentry/sentry-python/issues/273
    from rssant.settings import IS_CELERY_PROCESS, ENV_CONFIG
    if not IS_CELERY_PROCESS or not ENV_CONFIG.sentry_enable:
        return
    sentry_dsn = ENV_CONFIG.sentry_dsn
    if sentry_dsn:
        print('Sentry for celery enabled, PID={}!'.format(os.getpid()))
    client = Client(sentry_dsn)
    # register a custom filter to filter out duplicate logs
    register_logger_signal(client)
    # The register_logger_signal function can also take an optional argument
    # `loglevel` which is the level used for the handler created.
    # Defaults to `logging.ERROR`
    register_logger_signal(client, loglevel=logging.INFO)
    # The register_signal function can also take an optional argument
    # `ignore_expected` which causes exception classes specified in Task.throws
    # to be ignored
    register_signal(client, ignore_expected=True)
