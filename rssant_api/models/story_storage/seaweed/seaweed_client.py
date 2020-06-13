import typing
from concurrent.futures import ThreadPoolExecutor
import requests

from rssant.middleware.seaweed_panel import SeaweedMetrics


class SeaweedError(Exception):
    """
    SeaweedError
    """


class SeaweedClient:
    def __init__(self, volume_url: str, thread_pool_size: int = 30, timeout: int = 3):
        self.volume_url = volume_url.rstrip('/')
        self.thread_pool_size = thread_pool_size
        self.timeout = timeout
        self._m_session: requests.Session = None
        self._m_executor: ThreadPoolExecutor = None

    @property
    def _session(self):
        self._init()
        return self._m_session

    @property
    def _executor(self):
        self._init()
        return self._m_executor

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
            response = self._session.post(url, files={'file': data}, timeout=self.timeout)
            if response.status_code not in (200, 201, 204):
                raise SeaweedError(self._err('put', fid, response))

    def get(self, fid: str) -> bytes:
        url = self._get_file_url(fid)
        with SeaweedMetrics.record('get'):
            return self._get_by_url(fid, url)

    def _get_by_url(self, fid, url: str) -> bytes:
        response = self._session.get(url, timeout=self.timeout)
        if response.status_code == 404:
            return None
        if response.status_code not in (200,):
            raise SeaweedError(self._err('get', fid, response))
        return response.content

    def batch_get(self, fid_s: typing.List[str]) -> typing.Dict[str, bytes]:
        result = {}
        if not fid_s:
            return result
        url_s = [self._get_file_url(fid) for fid in fid_s]
        fut_s = []
        with SeaweedMetrics.record('get', len(fid_s)):
            for fid, url in zip(fid_s, url_s):
                fut = self._executor.submit(self._get_by_url, fid, url)
                fut_s.append((fid, fut))
            for fid, fut in fut_s:
                result[fid] = fut.result()
        return result

    def delete(self, fid: str) -> None:
        url = self._get_file_url(fid)
        with SeaweedMetrics.record('delete'):
            response = self._session.delete(url, timeout=self.timeout)
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
        if self._m_executor:
            self._m_executor.shutdown(wait=False)

    def _init(self):
        if self._m_session is None:
            self._m_session = requests.Session()
            # requests get_environ_proxies is too slow
            self._m_session.trust_env = False
            pool_maxsize = 10 + self.thread_pool_size
            for scheme in ['http://', 'https://']:
                adapter = requests.adapters.HTTPAdapter(
                    pool_maxsize=pool_maxsize, pool_connections=pool_maxsize)
                self._m_session.mount(scheme, adapter)
        if self._m_executor is None:
            self._m_executor = ThreadPoolExecutor(
                self.thread_pool_size, thread_name_prefix='seaweed_client')

    def _err(self, method: str, fid: str, response: requests.Response) -> str:
        msg = f': {response.text}' if response.text else ''
        return (f'seaweed {method} failed, fid={fid} '
                f'status={response.status_code}{msg}')
