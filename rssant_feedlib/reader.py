import re
import socket
import ssl
import ipaddress
import logging
from urllib.parse import urlparse
from http import HTTPStatus

import requests

from .response import FeedResponse, FeedResponseStatus
from .response_builder import FeedResponseBuilder


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


class FeedReaderError(Exception):
    """FeedReaderError"""
    status = None


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


def is_ok_status(status):
    return status and 200 <= status <= 299


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
        if self.allow_private_address:
            return
        hostname = urlparse(url).hostname
        for ip in self._resolve_hostname(hostname):
            ip = ipaddress.ip_address(ip)
            if ip.is_private:
                raise PrivateAddressError(ip)

    def check_content_type(self, response):
        if self.allow_non_webpage:
            return
        if not is_ok_status(response.status_code):
            return
        content_type = response.headers.get('content-type')
        if not is_webpage(content_type):
            raise ContentTypeNotSupportError(
                f'content-type {content_type!r} not support')

    def _read_content(self, response: requests.Response):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg)
        content_length = 0
        content = bytearray()
        for data in response.iter_content(chunk_size=64 * 1024):
            content_length += len(data)
            if content_length > self.max_content_length:
                msg = 'content length larger than limit {}'.format(
                    self.max_content_length)
                raise ContentTooLargeError(msg)
            content.extend(data)
        return content

    def _decode_content(self, content: bytes):
        if not content:
            return ''
        return content.decode('utf-8', errors='ignore')

    def _prepare_headers(self, etag=None, last_modified=None):
        headers = {'User-Agent': self.user_agent}
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        return headers

    def _send_request(self, request, ignore_content):
        # http://docs.python-requests.org/en/master/user/advanced/#timeouts
        response = self.session.send(request, timeout=(6.5, self.request_timeout), stream=True)
        try:
            if not is_ok_status(response.status_code):
                content = self._read_content(response)
                return response, content
            self.check_content_type(response)
            content = None
            if not ignore_content:
                content = self._read_content(response)
        finally:
            # Fix: Requests memory leak
            # https://github.com/psf/requests/issues/4601
            response.close()
        return response, content

    def _read(self, url, etag=None, last_modified=None, ignore_content=False):
        headers = self._prepare_headers(etag=etag, last_modified=last_modified)
        req = requests.Request('GET', url, headers=headers)
        prepared = self.session.prepare_request(req)
        if not self.allow_private_address:
            self.check_private_address(prepared.url)
        response, content = self._send_request(prepared, ignore_content=ignore_content)
        return response.headers, content, response.url, response.status_code

    def _read_by_proxy(self, url, etag=None, last_modified=None, ignore_content=False):
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
        response, content = self._send_request(prepared, ignore_content=ignore_content)
        if not is_ok_status(response.status_code):
            message = 'status={} body={!r}'.format(
                response.status_code, self._decode_content(content))
            raise RSSProxyError(message)
        proxy_status = response.headers.get('x-rss-proxy-status', None)
        if proxy_status and proxy_status.upper() == 'ERROR':
            message = 'status={} body={!r}'.format(
                response.status_code, self._decode_content(content))
            raise RSSProxyError(message)
        proxy_status = int(proxy_status) if proxy_status else HTTPStatus.OK.value
        return response.headers, content, url, proxy_status

    def read(self, url, *args, use_proxy=False, **kwargs) -> FeedResponse:
        headers = content = None
        try:
            if use_proxy:
                headers, content, url, status = self._read_by_proxy(url, *args, **kwargs)
            else:
                headers, content, url, status = self._read(url, *args, **kwargs)
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
        except UnicodeDecodeError:
            status = FeedResponseStatus.CONTENT_DECODING_ERROR.value
        except FeedReaderError as ex:
            status = ex.status
            LOG.warning(type(ex).__name__ + " url=%s %s", url, ex)
        except (requests.HTTPError, requests.RequestException) as ex:
            if ex.response is not None:
                status = ex.response.status_code
            else:
                status = FeedResponseStatus.UNKNOWN_ERROR.value
        builder = FeedResponseBuilder(use_proxy=use_proxy)
        builder.url(url)
        builder.status(status)
        builder.content(content)
        builder.headers(headers)
        return builder.build()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self._close_session:
            self.session.close()
