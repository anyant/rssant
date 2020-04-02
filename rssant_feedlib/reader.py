import re
import enum
import socket
import ssl
import ipaddress
import logging
from urllib.parse import urlparse
from http import HTTPStatus

import requests

from rssant_config import CONFIG
from rssant_common.helper import resolve_response_encoding


LOG = logging.getLogger(__name__)


DEFAULT_RSSANT_USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/67.0.3396.87 Safari/537.36 RSSAnt/1.0'
)


DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Linux; Android 8.0.0; TA-1053 Build/OPR1.170623.026) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3368.0 Mobile Safari/537.36'
)


class FeedResponseStatus(enum.Enum):
    # http://docs.python-requests.org/en/master/_modules/requests/exceptions/
    UNKNOWN_ERROR = -100
    CONNECTION_ERROR = -200
    PROXY_ERROR = -300
    RSS_PROXY_ERROR = -301
    RESPONSE_ERROR = -400
    DNS_ERROR = -201
    PRIVATE_ADDRESS_ERROR = -202
    CONNECTION_TIMEOUT = -203
    SSL_ERROR = -204
    READ_TIMEOUT = -205
    CONNECTION_RESET = -206
    TOO_MANY_REDIRECT_ERROR = -401
    CHUNKED_ENCODING_ERROR = -402
    CONTENT_DECODING_ERROR = -403
    CONTENT_TOO_LARGE_ERROR = -404
    REFERER_DENY = -405  # 严格防盗链，必须服务端才能绕过
    REFERER_NOT_ALLOWED = -406  # 普通防盗链，不带Referer头可绕过
    CONTENT_TYPE_NOT_SUPPORT_ERROR = -407  # 非文本/HTML响应

    @classmethod
    def name_of(cls, value):
        """
        >>> FeedResponseStatus.name_of(200)
        'OK'
        >>> FeedResponseStatus.name_of(-200)
        'RSSANT_CONNECTION_ERROR'
        >>> FeedResponseStatus.name_of(-999)
        'RSSANT_E999'
        """
        if value > 0:
            try:
                return HTTPStatus(value).name
            except ValueError:
                # eg: http://huanggua.sinaapp.com/
                # ValueError: 600 is not a valid HTTPStatus
                return f'HTTP_{value}'
        else:
            try:
                return 'RSSANT_' + FeedResponseStatus(value).name
            except ValueError:
                return f'RSSANT_E{abs(value)}'

    @classmethod
    def is_need_proxy(cls, value):
        return value in _NEED_PROXY_STATUS_SET


_NEED_PROXY_STATUS_SET = {x.value for x in [
    FeedResponseStatus.CONNECTION_ERROR,
    FeedResponseStatus.DNS_ERROR,
    FeedResponseStatus.CONNECTION_TIMEOUT,
    FeedResponseStatus.READ_TIMEOUT,
    FeedResponseStatus.CONNECTION_RESET,
]}


class FeedReaderError(Exception):
    """FeedReaderError"""
    status = None

    def __init__(self, *args, response=None, **kwargs):
        self.response = response
        super().__init__(*args, **kwargs)


class PrivateAddressError(FeedReaderError):
    """Private IP address"""
    status = FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value


class ContentTooLargeError(FeedReaderError):
    """Content too large"""
    status = FeedResponseStatus.CONTENT_TOO_LARGE_ERROR.value


class ContentTypeNotSupportError(FeedReaderError):
    """ContentTypeNotSupportError"""
    status = FeedResponseStatus.CONTENT_TYPE_NOT_SUPPORT_ERROR.value


class RSSProxyError(FeedReaderError):
    """RSSProxyError"""
    status = FeedResponseStatus.RSS_PROXY_ERROR.value


RE_WEBPAGE_CONTENT_TYPE = re.compile(
    r'(text/html|application/xml|text/xml|application/json|'
    r'application/.*xml|application/.*json|text/.*xml)', re.I)


def is_webpage(content_type):
    """
    >>> is_webpage(' text/HTML ')
    True
    >>> is_webpage('application/rss+xml; charset=utf-8')
    True
    >>> is_webpage('application/atom+json')
    True
    >>> is_webpage('image/jpeg')
    False
    """
    if not content_type:
        return True
    content_type = content_type.split(';', maxsplit=1)[0].strip()
    return bool(RE_WEBPAGE_CONTENT_TYPE.fullmatch(content_type))


class FeedReader:
    def __init__(
        self,
        session=None,
        user_agent=DEFAULT_USER_AGENT,
        request_timeout=30,
        max_content_length=10 * 1024 * 1024,
        allow_private_address=False,
        allow_non_webpage=False,
        rss_proxy_url=None,
        rss_proxy_token=None,
    ):
        if session is None:
            session = requests.session()
            self._close_session = True
        else:
            self._close_session = False
        self.session = session
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.max_content_length = max_content_length
        self.allow_private_address = allow_private_address
        self.allow_non_webpage = allow_non_webpage
        self.rss_proxy_url = rss_proxy_url
        self.rss_proxy_token = rss_proxy_token

    @property
    def has_rss_proxy(self):
        return bool(self.rss_proxy_url)

    def _resolve_hostname(self, hostname):
        addrinfo = socket.getaddrinfo(hostname, None)
        for family, __, __, __, sockaddr in addrinfo:
            if family == socket.AF_INET:
                ip, __ = sockaddr
                yield ip
            elif family == socket.AF_INET6:
                ip, __, __, __ = sockaddr
                yield ip

    def check_private_address(self, url):
        """Prevent request private address, which will attack local network"""
        if CONFIG.allow_private_address:
            return
        hostname = urlparse(url).hostname
        for ip in self._resolve_hostname(hostname):
            ip = ipaddress.ip_address(ip)
            if ip.is_private:
                raise PrivateAddressError(ip)

    def check_content_type(self, response):
        if self.allow_non_webpage:
            return
        if not (200 <= response.status_code <= 299):
            return
        content_type = response.headers.get('content-type')
        if not is_webpage(content_type):
            raise ContentTypeNotSupportError(
                f'content-type {content_type} not support', response=response)

    def _read_content(self, response: requests.Response):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'Content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg, response=response)
        content_length = 0
        content = bytearray()
        for data in response.iter_content(chunk_size=64 * 1024):
            content_length += len(data)
            if content_length > self.max_content_length:
                msg = 'Content length larger than limit {}'.format(self.max_content_length)
                raise ContentTooLargeError(msg, response=response)
            content.extend(data)
        response._content = bytes(content)

    def _prepare_headers(self, etag=None, last_modified=None):
        headers = {'User-Agent': self.user_agent}
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        return headers

    def _send_request(self, request):
        # http://docs.python-requests.org/en/master/user/advanced/#timeouts
        response = self.session.send(request, timeout=(6.5, self.request_timeout), stream=True)
        try:
            response.raise_for_status()
            self.check_content_type(response)
            self._read_content(response)
            resolve_response_encoding(response)
        finally:
            # Fix: Requests memory leak
            # https://github.com/psf/requests/issues/4601
            response.close()
        return response

    def _read(self, url, etag=None, last_modified=None):
        headers = self._prepare_headers(etag=etag, last_modified=last_modified)
        req = requests.Request('GET', url, headers=headers)
        prepared = self.session.prepare_request(req)
        if not self.allow_private_address:
            self.check_private_address(prepared.url)
        response = self._send_request(prepared)
        return response

    def _read_by_proxy(self, url, etag=None, last_modified=None):
        if not self.has_rss_proxy:
            raise ValueError("rss_proxy_url not provided")
        headers = self._prepare_headers(etag=etag, last_modified=last_modified)
        data = dict(
            url=url,
            token=self.rss_proxy_token,
            headers=headers,
        )
        req = requests.Request('POST', self.rss_proxy_url, json=data)
        prepared = self.session.prepare_request(req)
        try:
            response = self._send_request(prepared)
        except requests.HTTPError as ex:
            response.url = url
            status = ex.response.status_code
            message = ex.response.text
            LOG.error("rss-proxy error status=%s message=%s", status, message)
            raise RSSProxyError(status, message, response=ex.response) from ex
        response.url = url
        proxy_status = response.headers.get('x-rss-proxy-status', None)
        if proxy_status.upper() == 'ERROR':
            message = response.text
            raise RSSProxyError(proxy_status, message, response=response)
        response.status_code = int(proxy_status)
        response.raise_for_status()
        return response

    def read(self, *args, use_proxy=False, **kwargs):
        response = None
        try:
            if use_proxy:
                response = self._read_by_proxy(*args, **kwargs)
            else:
                response = self._read(*args, **kwargs)
        except socket.gaierror:
            status = FeedResponseStatus.DNS_ERROR.value
        except requests.exceptions.ReadTimeout:
            status = FeedResponseStatus.READ_TIMEOUT.value
        except (socket.timeout, TimeoutError, requests.exceptions.ConnectTimeout):
            status = FeedResponseStatus.CONNECTION_TIMEOUT.value
        except (ssl.SSLError, ssl.CertificateError, requests.exceptions.SSLError):
            status = FeedResponseStatus.SSL_ERROR.value
        except requests.exceptions.ProxyError:
            status = FeedResponseStatus.PROXY_ERROR.value
        except (ConnectionError, requests.exceptions.ConnectionError):
            status = FeedResponseStatus.CONNECTION_RESET.value
        except requests.exceptions.TooManyRedirects:
            status = FeedResponseStatus.TOO_MANY_REDIRECT_ERROR.value
        except requests.exceptions.ChunkedEncodingError:
            status = FeedResponseStatus.CHUNKED_ENCODING_ERROR.value
        except requests.exceptions.ContentDecodingError:
            status = FeedResponseStatus.CONTENT_DECODING_ERROR.value
        except FeedReaderError as ex:
            status = ex.status
            response = ex.response
        except requests.HTTPError as ex:
            response = ex.response
            status = response.status_code
        except requests.RequestException as ex:
            response = ex.response
            if response is not None:
                status = response.status_code
            else:
                status = FeedResponseStatus.UNKNOWN_ERROR.value
        else:
            status = response.status_code
        return status, response

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self._close_session:
            self.session.close()
