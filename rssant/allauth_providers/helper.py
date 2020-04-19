import logging
from urllib.parse import urlsplit, urlunsplit

import requests

from rssant_config import CONFIG


LOG = logging.getLogger(__name__)


def _default_user_agent():
    _requests_ua = requests.utils.default_user_agent()
    return '{}; rssant-proxy/1.0'.format(_requests_ua)


_DEFAULT_USER_AGENT = _default_user_agent()


class GitHubApiProxyError(requests.exceptions.RequestException):
    """GitHubApiProxyError"""


def _get_log_url(url):
    scheme, netloc, path, query, fragment = urlsplit(url)
    log_url = urlunsplit((scheme, netloc, path, None, None))
    return log_url


def _log_response(method, url, response):
    log_url = _get_log_url(url)
    is_ok = 200 <= response.status_code <= 299
    if is_ok:
        msg = f'status={response.status_code}'
    else:
        msg = f'status={response.status_code}: {response.text!r}'
    LOG.info(f'{method} {log_url} %s', msg)


def _log_exception(method, url, ex):
    log_url = _get_log_url(url)
    LOG.warning(f'{method} {log_url} %r', ex)


def github_api_request_by_proxy(method, url, timeout=None, **kwargs):
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
            raise GitHubApiProxyError(msg)
    headers = dict(request.headers)
    headers['user-agent'] = _DEFAULT_USER_AGENT
    proxy_data = {
        'method': request.method,
        'token': CONFIG.rss_proxy_token,
        'url': request.url,
        'headers': headers,
        'body': request_body,
    }
    response = requests.post(CONFIG.rss_proxy_url, json=proxy_data, timeout=timeout)
    proxy_status = response.headers.get('x-rss-proxy-status', None)
    if response.status_code != 200 or proxy_status == 'ERROR':
        msg = 'status={} {}'.format(response.status_code, response.text)
        raise GitHubApiProxyError(msg)
    response.status_code = int(proxy_status)
    return response


_RequestNetworkErrors = (
    ConnectionError,
    TimeoutError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


_DEFAULT_TIMEOUT = (3.5, 10)


def oauth_api_request(method, url, **kwargs):
    """
    when network error, fallback to use rss proxy
    """
    try:
        response = requests.request(method, url, **kwargs, timeout=_DEFAULT_TIMEOUT)
    except _RequestNetworkErrors as ex:
        _log_exception(method, url, ex)
        use_proxy = CONFIG.rss_proxy_enable and 'github.com' in url
        if not use_proxy:
            raise
        try:
            response = github_api_request_by_proxy(method, url, **kwargs, timeout=_DEFAULT_TIMEOUT)
        except (*_RequestNetworkErrors, GitHubApiProxyError) as ex:
            _log_exception(method, url, ex)
            raise
    _log_response(method, url, response)
    return response
