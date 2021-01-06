from urllib.parse import urlparse
import logging
import requests
from .requests_helper import requests_check_incomplete_response


LOG = logging.getLogger(__name__)


class KongClient:
    def __init__(self, url_prefix='http://localhost:8001'):
        self.url_prefix = url_prefix.rstrip('/')
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _url(self, path):
        return self.url_prefix + path

    def _check_status(self, response):
        try:
            response.raise_for_status()
            requests_check_incomplete_response(response)
        except requests.RequestException as ex:
            LOG.warning(f'{ex}: response={response.text}')
            raise

    def register(self, name, url):
        data = dict(name=name, url=url)
        res = self.session.put(self._url(f'/services/{name}'), json=data)
        self._check_status(res)
        service_id = res.json()['id']
        subpath = urlparse(url).path
        data = dict(paths=[subpath], service=dict(id=service_id))
        res = self.session.put(self._url(f'/routes/{name}'), json=data)
        self._check_status(res)

    def unregister(self, name):
        res = self.session.delete(self._url(f'/routes/{name}'))
        self._check_status(res)
        res = self.session.delete(self._url(f'/services/{name}'))
        self._check_status(res)
