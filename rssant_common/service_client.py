import httpx

from rssant_config import CONFIG


class RssantServiceClient:
    def __init__(
        self,
        *,
        worker_url: str,
        harbor_url: str,
        service_secret: str,
    ) -> None:
        self.worker_url = worker_url
        self.harbor_url = harbor_url
        self.service_secret = service_secret
        self._http_client = None
        self._async_http_client = None

    def _create_http_client(self):
        headers = {'x-rssant-service-secret': self.service_secret}
        return httpx.Client(
            headers=headers,
            trust_env=False,
            timeout=30,
        )

    @property
    def http_client(self):
        if self._http_client is None:
            self._http_client = self._create_http_client()
        return self._http_client

    def _create_async_http_client(self):
        headers = {'x-rssant-service-secret': self.service_secret}
        return httpx.AsyncClient(
            headers=headers,
            trust_env=False,
            timeout=30,
        )

    @property
    def async_http_client(self):
        if self._async_http_client is None:
            self._async_http_client = self._create_async_http_client()
        return self._async_http_client

    def close(self):
        if self._http_client is not None:
            self._http_client.close()
            self._http_client = None

    async def aclose(self):
        if self._async_http_client is not None:
            await self._async_http_client.aclose()
            self._async_http_client = None

    def _get_api_url(self, api: str):
        if api.startswith('worker'):
            base_url = self.worker_url
        elif api.startswith('harbor'):
            base_url = self.harbor_url
        else:
            raise ValueError(f'unknown api: {api}')
        return base_url.rstrip('/') + '/api/v1/' + api.lstrip('/')

    def call(self, api: str, data: dict = None, timeout: int = None):
        url = self._get_api_url(api)
        resp = self.http_client.post(url, json=data, timeout=timeout)
        resp.raise_for_status()
        if not resp.text:
            return None
        return resp.json()

    async def acall(self, api: str, data: dict = None, timeout: int = None):
        url = self._get_api_url(api)
        resp = await self.async_http_client.post(url, json=data, timeout=timeout)
        resp.raise_for_status()
        if not resp.text:
            return None
        return resp.json()


SERVICE_CLIENT = RssantServiceClient(
    worker_url=CONFIG.worker_url,
    harbor_url=CONFIG.harbor_url,
    service_secret=CONFIG.service_secret,
)
