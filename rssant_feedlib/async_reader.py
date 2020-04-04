import socket
import ssl
import asyncio
import logging
import typing
import concurrent.futures
import ipaddress
from urllib.parse import urlparse

import aiodns
import aiohttp
import yarl

from rssant_config import CONFIG
from rssant_common.helper import (
    resolve_aiohttp_response_encoding,
    aiohttp_raise_for_status,
    aiohttp_client_session,
)

from .reader import DEFAULT_USER_AGENT, FeedResponseStatus, is_webpage
from .reader import (
    PrivateAddressError,
    ContentTooLargeError,
    ContentTypeNotSupportError,
    RSSProxyError,
    FeedReaderError,
)


LOG = logging.getLogger(__name__)


class AsyncFeedReader:
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
        self._close_session = session is None
        self.session = session
        self.resolver = None
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

    async def _async_init(self):
        if self.resolver is None:
            self.resolver = aiodns.DNSResolver(loop=asyncio.get_event_loop())
        if self.session is None:
            self.session = aiohttp_client_session(timeout=self.request_timeout)

    async def _resolve_hostname(self, hostname):
        """
        Note on addrinfo:
        # https://pycares.readthedocs.io/en/latest/channel.html#pycares.Channel.query
        # extra type ares_host_result: addresses, aliases, name
        # example: <ares_host_result> name=fn0wz54v.dayugslb.com, aliases=['gitee.com'], addresses=['180.97.125.228']
        # example dig gitee.com:
        ;; QUESTION SECTION:
        ;gitee.com.			IN	A

        ;; ANSWER SECTION:
        gitee.com.		300	IN	CNAME	fn0wz54v.dayugslb.com.
        fn0wz54v.dayugslb.com.	300	IN	A	180.97.125.228
        """
        addrinfo = await self.resolver.gethostbyname(hostname, socket.AF_INET)
        if getattr(addrinfo, 'addresses', None):
            for ip in addrinfo.addresses:
                yield ip
        elif getattr(addrinfo, 'host', None):
            yield addrinfo.host

    async def check_private_address(self, url):
        """Prevent request private address, which will attack local network"""
        if CONFIG.allow_private_address:
            return
        await self._async_init()
        hostname = urlparse(url).hostname
        async for ip in self._resolve_hostname(hostname):
            ip = ipaddress.ip_address(ip)
            if ip.is_private:
                raise PrivateAddressError(ip)

    def check_content_type(self, response):
        if self.allow_non_webpage:
            return
        if not (200 <= response.status <= 299):
            return
        content_type = response.headers.get('content-type')
        if not is_webpage(content_type):
            raise ContentTypeNotSupportError(
                f'content-type {content_type} not support', response=response)

    async def _read_content(self, response):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'Content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg, response=response)
        content_length = 0
        content = []
        async for chunk in response.content.iter_chunked(8 * 1024):
            content_length += len(chunk)
            if content_length > self.max_content_length:
                msg = 'Content length larger than limit {}'.format(self.max_content_length)
                raise ContentTooLargeError(msg, response=response)
            content.append(chunk)
        content = b''.join(content)
        response._body = content
        encoding = await resolve_aiohttp_response_encoding(response, content)
        text = content.decode(encoding)
        response.rssant_encoding = encoding
        response.rssant_content = content
        response.rssant_text = text

    def _prepare_headers(self, etag=None, last_modified=None, referer=None, headers=None):
        if headers is None:
            headers = {}
        headers['User-Agent'] = self.user_agent
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        if referer:
            headers["Referer"] = referer
        return headers

    async def _read_by_proxy(
            self, url, etag=None, last_modified=None, referer=None,
            headers=None, ignore_content=False) -> aiohttp.ClientResponse:
        if not self.has_rss_proxy:
            raise ValueError("rss_proxy_url not provided")
        headers = self._prepare_headers(
            etag=etag,
            last_modified=last_modified,
            referer=referer,
            headers=headers,
        )
        data = dict(
            url=url,
            token=self.rss_proxy_token,
            headers=headers,
        )
        await self._async_init()
        async with self.session.post(self.rss_proxy_url, json=data) as response:
            response: aiohttp.ClientResponse
            # TODO: avoid use private field response._url
            url_obj = yarl.URL(url).with_fragment(None)
            response._url = url_obj
            response._cache['url'] = url_obj
            if response.status != 200:
                status = response.status
                message = await response.text()
                LOG.error("rss-proxy error status=%s message=%s", status, message)
                raise RSSProxyError(status, message, response=response)
            proxy_status = response.headers.get('x-rss-proxy-status', None)
            if proxy_status.upper() == 'ERROR':
                message = await response.text()
                raise RSSProxyError(proxy_status, message, response=response)
            response.status = int(proxy_status)
            aiohttp_raise_for_status(response)
            self.check_content_type(response)
            if not ignore_content:
                await self._read_content(response)
        return response

    async def _read(
            self, url, etag=None, last_modified=None, referer=None,
            headers=None, ignore_content=False) -> aiohttp.ClientResponse:
        headers = self._prepare_headers(
            etag=etag,
            last_modified=last_modified,
            referer=referer,
            headers=headers,
        )
        await self._async_init()
        if not self.allow_private_address:
            await self.check_private_address(url)
        async with self.session.get(url, headers=headers) as response:
            aiohttp_raise_for_status(response)
            self.check_content_type(response)
            if not ignore_content:
                await self._read_content(response)
        return response

    async def read(self, *args, use_proxy=False, **kwargs) -> typing.Tuple[int, aiohttp.ClientResponse]:
        response = None
        try:
            if use_proxy:
                response = await self._read_by_proxy(*args, **kwargs)
            else:
                response = await self._read(*args, **kwargs)
        except (socket.gaierror, aiodns.error.DNSError):
            status = FeedResponseStatus.DNS_ERROR.value
        except (socket.timeout, TimeoutError, aiohttp.ServerTimeoutError,
                asyncio.TimeoutError, concurrent.futures.TimeoutError):
            status = FeedResponseStatus.CONNECTION_TIMEOUT.value
        except (ssl.SSLError, ssl.CertificateError,
                aiohttp.ServerFingerprintMismatch,
                aiohttp.ClientSSLError,
                aiohttp.ClientConnectorSSLError,
                aiohttp.ClientConnectorCertificateError):
            status = FeedResponseStatus.SSL_ERROR.value
        except (aiohttp.ClientProxyConnectionError,
                aiohttp.ClientHttpProxyError):
            status = FeedResponseStatus.PROXY_ERROR.value
        except (ConnectionError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ServerConnectionError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientConnectorError):
            status = FeedResponseStatus.CONNECTION_RESET.value
        except (aiohttp.WSServerHandshakeError, aiohttp.ClientOSError):
            status = FeedResponseStatus.CONNECTION_ERROR.value
        except aiohttp.ClientPayloadError:
            status = FeedResponseStatus.CHUNKED_ENCODING_ERROR.value
        except UnicodeDecodeError:
            status = FeedResponseStatus.CONTENT_DECODING_ERROR.value
        except FeedReaderError as ex:
            status = ex.status
            response = ex.response
        except (aiohttp.ClientResponseError, aiohttp.ContentTypeError) as ex:
            status = ex.status
            if ex.history:
                response = ex.history[-1]
        except (aiohttp.ClientError, aiohttp.InvalidURL):
            status = FeedResponseStatus.UNKNOWN_ERROR.value
        else:
            status = response.status
        if response:
            if not hasattr(response, 'rssant_encoding'):
                response.rssant_encoding = None
            if not hasattr(response, 'rssant_content'):
                response.rssant_content = b''
            if not hasattr(response, 'rssant_text'):
                response.rssant_text = ''
        return status, response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await self.close()

    async def close(self):
        if self._close_session and self.session is not None:
            await self.session.close()
