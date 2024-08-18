import random

from rssant_config import CONFIG

from .blacklist import compile_url_blacklist
from .ezproxy import EZPROXY_SERVICE

USE_PROXY_URL_LIST = '''
tandfonline.com
sagepub.com
cnki.net
'''

_is_use_proxy_url = compile_url_blacklist(USE_PROXY_URL_LIST)


def is_use_proxy_url(url):
    """
    >>> is_use_proxy_url('https://navi.cnki.net/knavi/rss/SHXJ')
    True
    """
    return _is_use_proxy_url(url)


def choice_proxy(*, proxy_url, rss_proxy_url) -> bool:
    if proxy_url and rss_proxy_url:
        use_rss_proxy = random.random() > 0.67
    else:
        use_rss_proxy = bool(rss_proxy_url)
    return use_rss_proxy


def get_proxy_options(url: str = None) -> dict:
    options = {}
    if CONFIG.proxy_enable:
        options.update(proxy_url=CONFIG.proxy_url)
    elif CONFIG.ezproxy_enable:
        proxy_url = EZPROXY_SERVICE.pick_proxy(url=url)
        if proxy_url:
            options.update(proxy_url=proxy_url)
            # 特殊处理的URL，不使用RSS代理
            if url and is_use_proxy_url(url):
                return options
    if CONFIG.rss_proxy_enable:
        options.update(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )
    return options
