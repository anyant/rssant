import random
from rssant_config import CONFIG


def choice_proxy(*, proxy_url, rss_proxy_url) -> bool:
    if proxy_url and rss_proxy_url:
        use_rss_proxy = random.random() > 0.333
    else:
        use_rss_proxy = bool(rss_proxy_url)
    return use_rss_proxy


def get_proxy_options() -> dict:
    options = {}
    if CONFIG.proxy_enable:
        options.update(proxy_url=CONFIG.proxy_url)
    if CONFIG.rss_proxy_enable:
        options.update(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )
    return options
