from threading import Lock
from typing import List
from urllib.parse import urlparse

import httpx
from cachetools import TTLCache, cached

from rssant_common.chnlist import CHINA_WEBSITE_LIST
from rssant_config import CONFIG


class EzproxyClient:
    def __init__(self, base_url: str, apikey: str) -> None:
        self._base_url = base_url
        self._apikey = apikey

    def _url_for(self, api: str) -> str:
        base = self._base_url.rstrip('/') + '/api/v1/'
        return base + api.lstrip('/')

    def _call(self, api: str, **kwargs):
        url = self._url_for(api)
        headers = {'x-api-key': self._apikey}
        resp = httpx.post(url, json=kwargs, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def pick_proxy(
        self,
        *,
        seed: str,
        region_s: List[str] = None,
        chain: str = None,
        count: int = 1,
    ) -> List[dict]:
        result = self._call(
            'proxy.pick',
            chain=chain,
            seed=seed,
            region_s=region_s,
            count=count,
        )
        return result['item_s']

    def pick_proxy_url(
        self,
        *,
        seed: str,
        chain: str = None,
        region_s: List[str] = None,
    ) -> str:
        item_s = self.pick_proxy(
            chain=chain,
            seed=seed,
            region_s=region_s,
            count=1,
        )
        if not item_s:
            return None
        return item_s[0]['url']


class EZProxyService:
    def __init__(self) -> None:
        self._client = None
        if CONFIG.ezproxy_enable:
            self._client = EzproxyClient(
                base_url=CONFIG.ezproxy_base_url,
                apikey=CONFIG.ezproxy_apikey,
            )

    def _get_client(self):
        if self._client is None:
            raise ValueError('ezproxy is not enable')
        return self._client

    @cached(
        cache=TTLCache(ttl=10, maxsize=10000),
        lock=Lock(),
    )
    def _pick_proxy_url(self, *, seed: str, chain: str = None) -> str:
        return self._get_client().pick_proxy_url(chain=chain, seed=seed)

    def pick_proxy(self, *, url: str = None) -> str:
        if not url:
            return self._pick_proxy_url(seed='rssant')
        hostname = urlparse(url).hostname
        if CHINA_WEBSITE_LIST.is_china_website(hostname):
            chain = CONFIG.ezproxy_chain_cn
        else:
            chain = CONFIG.ezproxy_chain_global
        return self._pick_proxy_url(chain=chain, seed=hostname)


EZPROXY_SERVICE = EZProxyService()
