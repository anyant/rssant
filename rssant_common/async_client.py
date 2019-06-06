from rssant.settings import ENV_CONFIG
from rssant_async.client import RssantAsyncClient


def create_async_client():
    return RssantAsyncClient(
        ENV_CONFIG.async_url_prefix, ENV_CONFIG.async_callback_url_prefix)


async_client = create_async_client()
