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

    def http_client(self, timeout=None):
        if timeout is None:
            timeout = 30
        headers = {'x-rssant-service-secret': self.service_secret}
        return httpx.Client(
            headers=headers,
            trust_env=False,
            timeout=timeout,
        )

    def async_http_client(self, timeout=None):
        if timeout is None:
            timeout = 30
        headers = {'x-rssant-service-secret': self.service_secret}
        return httpx.AsyncClient(
            headers=headers,
            trust_env=False,
            timeout=timeout,
        )

    def _get_api_url(self, api: str):
        if api.startswith('worker'):
            base_url = self.worker_url
        elif api.startswith('harbor'):
            base_url = self.harbor_url
        else:
            raise ValueError(f'unknown api: {api}')
        return base_url.rstrip('/') + '/api/v1' + api.lstrip('/')

    def call(self, api: str, data: dict = None, timeout: int = None):
        url = self._get_api_url(api)
        with self.http_client(timeout=timeout) as client:
            resp = client.post(url, json=data)
            resp.raise_for_status()
            return resp.json()

    async def acall(self, api: str, data: dict = None, timeout: int = None):
        url = self._get_api_url(api)
        async with self.async_http_client(timeout=timeout) as client:
            resp = await client.post(url, json=data)
            resp.raise_for_status()
            return resp.json()


SERVICE_CLIENT = RssantServiceClient(
    worker_url=CONFIG.worker_url,
    harbor_url=CONFIG.harbor_url,
    service_secret=CONFIG.service_secret,
)
