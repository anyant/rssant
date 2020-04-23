import logging

from rssant_config import CONFIG
from rssant_common.rss_proxy import RSSProxyClient, ProxyStrategy


LOG = logging.getLogger(__name__)


def _proxy_strategy(url):
    if 'github.com' in url:
        return ProxyStrategy.DIRECT_FIRST
    else:
        return ProxyStrategy.DIRECT


def oauth_api_request(method, url, **kwargs):
    """
    when network error, fallback to use rss proxy
    """
    client = RSSProxyClient(
        rss_proxy_url=CONFIG.rss_proxy_url,
        rss_proxy_token=CONFIG.rss_proxy_token,
        proxy_strategy=_proxy_strategy,
    )
    return client.request(method, url, **kwargs)
