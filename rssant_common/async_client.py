from rssant_config import CONFIG
from rssant_async.client import RssantAsyncClient


def create_async_client():
    return RssantAsyncClient(
        CONFIG.async_url_prefix, CONFIG.async_callback_url_prefix)


async_client = create_async_client()
