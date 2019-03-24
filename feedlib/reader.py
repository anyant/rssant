import enum
import socket
import ssl
import ipaddress
from urllib.parse import urlparse

import requests

from common.helper import resolve_response_encoding


DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/67.0.3396.87 Safari/537.36 RSSAnt/1.0'
)


class PrivateAddressError(Exception):
    """Private IP address"""


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


class FeedReader:
    def __init__(self, session=None, user_agent=DEFAULT_USER_AGENT, request_timeout=30):
        if session is None:
            session = requests.session()
            self._close_session = True
        else:
            self._close_session = False
        self.session = session
        self.user_agent = user_agent
        self.request_timeout = request_timeout

    def _check_private_ip(self, url):
        """Prevent request private address, which will attack local network"""
        hostname = urlparse(url).hostname
        addrinfo = socket.getaddrinfo(hostname, None)
        for family, __, __, __, sockaddr in addrinfo:
            if family == socket.AF_INET:
                ip, __ = sockaddr
                ip = ipaddress.IPv4Address(ip)
            elif family == socket.AF_INET6:
                ip, __, __, __ = sockaddr
                ip = ipaddress.IPv6Address(ip)
            else:
                continue
            if ip.is_private:
                raise PrivateAddressError(ip)

    def _read(self, url, etag=None, last_modified=None):
        headers = {'User-Agent': self.user_agent}
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        req = requests.Request('GET', url, headers=headers)
        prepared = self.session.prepare_request(req)
        self._check_private_ip(prepared.url)
        # http://docs.python-requests.org/en/master/user/advanced/#timeouts
        response = self.session.send(prepared, timeout=(6.5, self.request_timeout))
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
        except (ssl.SSLError, requests.exceptions.SSLError):
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
