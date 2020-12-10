import logging

from rssant_common import _proxy_helper
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
    options = _proxy_helper.get_proxy_options()
    client = RSSProxyClient(**options, proxy_strategy=_proxy_strategy)
    return client.request(method, url, **kwargs)
