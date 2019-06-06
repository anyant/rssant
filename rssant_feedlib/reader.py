import enum
import socket
import ssl
import ipaddress
from urllib.parse import urlparse

import requests

from rssant_common.helper import resolve_response_encoding


DEFAULT_RSSANT_USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/67.0.3396.87 Safari/537.36 RSSAnt/1.0'
)


DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Mobile Safari/537.36'
)


class PrivateAddressError(Exception):
    """Private IP address"""


class ContentTooLargeError(Exception):
    """Content too large"""


class FeedResponseStatus(enum.Enum):
    # http://docs.python-requests.org/en/master/_modules/requests/exceptions/
    UNKNOWN_ERROR = -100
    CONNECTION_ERROR = -200
    PROXY_ERROR = -300
    RESPONSE_ERROR = -400
    DNS_ERROR = -201
    PRIVATE_ADDRESS_ERROR = -202
    CONNECTION_RESET = -202
    CONNECTION_TIMEOUT = - 203
    SSL_ERROR = - 204
    READ_TIMEOUT = -205
    TOO_MANY_REDIRECT_ERROR = -401
    CHUNKED_ENCODING_ERROR = -402
    CONTENT_DECODING_ERROR = -403
    CONTENT_TOO_LARGE_ERROR = -404


class FeedReader:
    def __init__(
        self,
        session=None,
        user_agent=DEFAULT_USER_AGENT,
        request_timeout=30,
        max_content_length=10 * 1024 * 1024,
        allow_private_address=False,
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
        hostname = urlparse(url).hostname
        for ip in self._resolve_hostname(hostname):
            ip = ipaddress.ip_address(ip)
            if ip.is_private:
                raise PrivateAddressError(ip)

    def _read_content(self, response):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'Content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg)
        content_length = 0
        content = []
        for data in response.iter_content(chunk_size=8 * 1024):
            content_length += len(data)
            if content_length > self.max_content_length:
                msg = 'Content length larger than limit {}'.format(self.max_content_length)
                raise ContentTooLargeError(msg)
            content.append(data)
        response._content = b''.join(content)

    def _read(self, url, etag=None, last_modified=None):
        headers = {'User-Agent': self.user_agent}
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        req = requests.Request('GET', url, headers=headers)
        prepared = self.session.prepare_request(req)
        if not self.allow_private_address:
            self.check_private_address(prepared.url)
        # http://docs.python-requests.org/en/master/user/advanced/#timeouts
        response = self.session.send(prepared, timeout=(6.5, self.request_timeout), stream=True)
        self._read_content(response)
        response.raise_for_status()
        resolve_response_encoding(response)
        return response

    def read(self, *args, **kwargs):
        response = None
        try:
            response = self._read(*args, **kwargs)
        except socket.gaierror:
            status = FeedResponseStatus.DNS_ERROR.value
        except PrivateAddressError:
            status = FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value
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
        except ContentTooLargeError:
            status = FeedResponseStatus.CONTENT_TOO_LARGE_ERROR.value
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
