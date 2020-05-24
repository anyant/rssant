import requests

from rssant.middleware.seaweed_panel import SeaweedMetrics


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

    def _get_file_url(self, fid: str):
        try:
            volume_id, rest = fid.strip().split(",")
        except ValueError:
            raise ValueError(
                "fid must be in format: <volume_id>,<file_key>")
        return self.volume_url + f'/{fid}'

    def put(self, fid: str, data: bytes) -> None:
        url = self._get_file_url(fid)
        with SeaweedMetrics.record('put'):
            response = self._session.post(url, files={'file': data})
            if response.status_code not in (200, 201, 204):
                raise SeaweedError(self._err('put', fid, response))

    def get(self, fid: str) -> bytes:
        url = self._get_file_url(fid)
        with SeaweedMetrics.record('get'):
            response = self._session.get(url)
            if response.status_code == 404:
                return None
            if response.status_code not in (200,):
                raise SeaweedError(self._err('get', fid, response))
            return response.content

    def delete(self, fid: str) -> None:
        url = self._get_file_url(fid)
        with SeaweedMetrics.record('delete'):
            response = self._session.delete(url)
            if response.status_code not in (200, 202, 204, 404):
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
        msg = f': {response.text}' if response.text else ''
        return (f'seaweed {method} failed, fid={fid} '
                f'status={response.status_code}{msg}')
