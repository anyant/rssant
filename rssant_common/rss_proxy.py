import socket
import logging
import enum
from urllib.parse import urlsplit, urlunsplit

import requests

from rssant_common import _proxy_helper


LOG = logging.getLogger(__name__)


def _default_user_agent():
    _requests_ua = requests.utils.default_user_agent()
    return '{}; rssant-proxy/1.0'.format(_requests_ua)


_DEFAULT_USER_AGENT = _default_user_agent()
_DEFAULT_TIMEOUT = (3.5, 10)


class RSSProxyClientError(requests.exceptions.RequestException):
    """RSSProxyClientError"""


def _get_log_url(url):
    scheme, netloc, path, query, fragment = urlsplit(url)
    log_url = urlunsplit((scheme, netloc, path, None, None))
    return log_url


def _log_response(method, url, response, use_proxy):
    log_url = _get_log_url(url)
    is_ok = 200 <= response.status_code <= 299
    if is_ok:
        msg = f'status={response.status_code}'
    else:
        msg = f'status={response.status_code}: {response.text!r}'
    proxy_info = '[proxy] ' if use_proxy else ''
    LOG.info(f'{proxy_info}{method} {log_url} %s', msg)


def _log_exception(method, url, ex, use_proxy):
    log_url = _get_log_url(url)
    proxy_info = '[proxy] ' if use_proxy else ''
    LOG.warning(f'{proxy_info}{method} {log_url} %r', ex)


_RequestNetworkErrors = (
    socket.gaierror,
    socket.timeout,
    ConnectionError,
    TimeoutError,
    requests.exceptions.Timeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ConnectionError,
)


class ProxyStrategy(enum.Enum):
    DIRECT = 'DIRECT'
    DIRECT_FIRST = 'DIRECT_FIRST'
    PROXY = 'PROXY'
    PROXY_FIRST = 'PROXY_FIRST'


class RSSProxyClient:
    def __init__(
        self,
        proxy_url=None,
        rss_proxy_url=None,
        rss_proxy_token=None,
        proxy_strategy=None,
    ):
        self.proxy_url = proxy_url
        self.rss_proxy_url = rss_proxy_url
        self.rss_proxy_token = rss_proxy_token
        self.proxy_strategy = self._get_proxy_strategy(proxy_strategy)

    @staticmethod
    def _get_proxy_strategy(strategy=None):
        if strategy is None:
            strategy = ProxyStrategy.DIRECT_FIRST
        if isinstance(strategy, str):
            strategy = ProxyStrategy(strategy)
        if isinstance(strategy, ProxyStrategy):
            return lambda url: strategy
        assert callable(strategy), 'strategy must be ProxyStrategy type or callable'
        return strategy

    @property
    def has_proxy(self):
        return bool(self.rss_proxy_url or self.proxy_url)

    def request_direct(self, method, url, timeout=None, **kwargs) -> requests.Response:
        if timeout is None:
            timeout = _DEFAULT_TIMEOUT
        response = requests.request(method, url, **kwargs, timeout=timeout)
        response.close()
        return response

    def request_by_proxy(self, *args, **kwargs) -> requests.Response:
        use_rss_proxy = _proxy_helper.choice_proxy(
            proxy_url=self.proxy_url, rss_proxy_url=self.rss_proxy_url)
        if use_rss_proxy:
            if not self.rss_proxy_url:
                raise ValueError("rss_proxy_url not provided")
            return self._request_by_rss_proxy(*args, **kwargs)
        else:
            if not self.proxy_url:
                raise ValueError("proxy_url not provided")
            proxies = {'http': self.proxy_url, 'https': self.proxy_url}
            return self.request_direct(*args, **kwargs, proxies=proxies)

    def _request_by_rss_proxy(self, method, url, timeout=None, **kwargs) -> requests.Response:
        if timeout is None:
            timeout = _DEFAULT_TIMEOUT
        request: requests.PreparedRequest
        request = requests.Request(method, url, **kwargs).prepare()
        request_body = None
        if request.body:
            if isinstance(request.body, bytes):
                request_body = request.body.decode('utf-8')
            elif isinstance(request.body, str):
                request_body = request.body
            else:
                msg = f'not support request body type {type(request.body).__name__}'
                raise ValueError(msg)
        headers = dict(request.headers)
        headers['user-agent'] = _DEFAULT_USER_AGENT
        proxy_data = {
            'method': request.method,
            'token': self.rss_proxy_token,
            'url': request.url,
            'headers': headers,
            'body': request_body,
        }
        response = requests.post(self.rss_proxy_url, json=proxy_data, timeout=timeout)
        response.close()
        proxy_status = response.headers.get('x-rss-proxy-status', None)
        if response.status_code != 200 or proxy_status == 'ERROR':
            msg = 'status={} {}'.format(response.status_code, response.text)
            raise RSSProxyClientError(msg)
        response.status_code = int(proxy_status)
        return response

    def request(self, method, url, timeout=None, **kwargs) -> requests.Response:
        if not self.has_proxy:
            return self.request_direct(method, url, timeout=timeout, **kwargs)
        request_first = request_second = None
        proxy_strategy = self.proxy_strategy(url)
        if proxy_strategy == ProxyStrategy.DIRECT:
            request_first = self.request_direct
        elif proxy_strategy == ProxyStrategy.PROXY:
            request_first = self.request_by_proxy
        elif proxy_strategy == ProxyStrategy.DIRECT_FIRST:
            request_first = self.request_direct
            request_second = self.request_by_proxy
        elif proxy_strategy == ProxyStrategy.PROXY_FIRST:
            request_first = self.request_by_proxy
            request_second = self.request_direct
        else:
            raise ValueError(f'unknown proxy strategy {proxy_strategy!r}')
        use_proxy = request_first == self.request_by_proxy
        try:
            response = request_first(method, url, timeout=timeout, **kwargs)
        except (*_RequestNetworkErrors, RSSProxyClientError) as ex:
            _log_exception(method, url, ex, use_proxy)
            if not request_second:
                raise
            use_proxy = request_second == self.request_by_proxy
            try:
                response = request_second(method, url, timeout=timeout, **kwargs)
            except (*_RequestNetworkErrors, RSSProxyClientError) as ex:
                _log_exception(method, url, ex, use_proxy)
                raise
        _log_response(method, url, response, use_proxy)
        return response
