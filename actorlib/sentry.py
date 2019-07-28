import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration


def sentry_init(dsn=None):
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            integrations=[AioHttpIntegration()]
        )


sentry_scope = sentry_sdk.configure_scope
