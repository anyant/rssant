from rssant.settings import ENV_CONFIG
from rssant_async.client import RssantAsyncClient

async_client = RssantAsyncClient(
    ENV_CONFIG.async_url_prefix, ENV_CONFIG.async_callback_url_prefix)
