import requests


class SeaweedError(Exception):
    """
    SeaweedError
    """


class SeaweedClient:
    def __init__(self, volume_url: str):
        self.volume_url = volume_url.rstrip('/')
        self._m_session = None

    @property
    def _session(self):
        self._init()
        return self._m_session

    def put(self, fid: str, data: bytes) -> None:
        url = self.volume_url + f'/{fid}'
        response = self._session.post(url, files={'file': data})
        if response.status_code not in (200, 201):
            raise SeaweedError(self._err('put', fid, response))

    def get(self, fid: str) -> bytes:
        url = self.volume_url + f'/{fid}'
        response = self._session.get(url)
        if response.status_code == 404:
            return None
        if response.status_code not in (200,):
            raise SeaweedError(self._err('get', fid, response))
        return response.content

    def delete(self, fid: str) -> None:
        url = self.volume_url + f'/{fid}'
        response = self._session.delete(url)
        if response.status_code not in (200, 204):
            raise SeaweedError(self._err('delete', fid, response))

    def __enter__(self):
        self._init()
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self._m_session:
            self._m_session.close()

    def _init(self):
        if self._m_session is None:
            self._m_session = requests.Session()

    def _err(self, method: str, fid: str, response: requests.Response) -> str:
        return (f'seaweed {method} failed, fid={fid} '
                f'status={response.status_code}: {response.text}')
