import socket
import ssl
import asyncio
import logging
import concurrent.futures
import ipaddress
from http import HTTPStatus
from urllib.parse import urlparse

import aiodns
import aiohttp

from rssant_common.helper import aiohttp_client_session
from rssant_common.dns_service import DNSService, DNS_SERVICE

from .reader import is_webpage, is_ok_status
from .reader import (
    PrivateAddressError,
    ContentTooLargeError,
    ContentTypeNotSupportError,
    RSSProxyError,
    FeedReaderError,
)
from .response import FeedResponse, FeedResponseStatus
from .response_builder import FeedResponseBuilder
from .useragent import DEFAULT_USER_AGENT


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
        dns_service: DNSService = DNS_SERVICE,
    ):
        self._close_session = session is None
        self.session = session
        self.resolver: aiohttp.AsyncResolver = None
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.max_content_length = max_content_length
        self.allow_private_address = allow_private_address
        self.allow_non_webpage = allow_non_webpage
        self.rss_proxy_url = rss_proxy_url
        self.rss_proxy_token = rss_proxy_token
        self.dns_service = dns_service

    @property
    def has_rss_proxy(self):
        return bool(self.rss_proxy_url)

    async def _async_init(self):
        if self.resolver is None:
            loop = asyncio.get_event_loop()
            if self.dns_service is None:
                self.resolver = aiohttp.AsyncResolver(loop=loop)
            else:
                self.resolver = self.dns_service.aiohttp_resolver(loop=loop)
        if self.session is None:
            self.session = aiohttp_client_session(
                resolver=self.resolver, timeout=self.request_timeout)

    async def _resolve_hostname(self, hostname):
        try:
            hosts = await self.resolver.resolve(hostname, family=socket.AF_INET)
        except (aiodns.error.DNSError, OSError) as ex:
            LOG.info("resolve hostname %s failed %r", hostname, ex)
            hosts = []
        for item in hosts:
            yield item['host']

    async def check_private_address(self, url):
        """Prevent request private address, which will attack local network"""
        if self.allow_private_address:
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
        if not is_ok_status(response.status):
            return
        content_type = response.headers.get('content-type')
        if not is_webpage(content_type, str(response.url)):
            raise ContentTypeNotSupportError(
                f'content-type {content_type} not support')

    async def _read_content(self, response: aiohttp.ClientResponse):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg)
        content_length = 0
        content = bytearray()
        async for chunk in response.content.iter_chunked(8 * 1024):
            content_length += len(chunk)
            if content_length > self.max_content_length:
                msg = 'content length larger than limit {}'.format(
                    self.max_content_length)
                raise ContentTooLargeError(msg)
            content.extend(chunk)
        return content

    async def _read_text(self, response: aiohttp.ClientResponse):
        content = await self._read_content(response)
        return content.decode('utf-8', errors='ignore')

    def _prepare_headers(self, url, etag=None, last_modified=None, referer=None, headers=None):
        if headers is None:
            headers = {}
        if callable(self.user_agent):
            headers['User-Agent'] = self.user_agent(url)
        else:
            headers['User-Agent'] = self.user_agent
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        if referer:
            headers["Referer"] = referer
        return headers

    async def _read(
            self, url, etag=None, last_modified=None, referer=None,
            headers=None, ignore_content=False
    ) -> aiohttp.ClientResponse:
        headers = self._prepare_headers(
            url,
            etag=etag,
            last_modified=last_modified,
            referer=referer,
            headers=headers,
        )
        await self._async_init()
        if not self.allow_private_address:
            await self.check_private_address(url)
        async with self.session.get(url, headers=headers) as response:
            content = None
            if not is_ok_status(response.status) or not ignore_content:
                content = await self._read_content(response)
            if not is_ok_status(response.status):
                return response.headers, content, url, response.status
            self.check_content_type(response)
        return response.headers, content, str(response.url), response.status

    async def _read_by_proxy(
        self, url, etag=None, last_modified=None, referer=None,
        headers=None, ignore_content=False
    ):
        if not self.has_rss_proxy:
            raise ValueError("rss_proxy_url not provided")
        headers = self._prepare_headers(
            url,
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
            if not is_ok_status(response.status):
                body = await self._read_text(response)
                message = f'status={response.status} body={body!r}'
                raise RSSProxyError(message)
            proxy_status = response.headers.get('x-rss-proxy-status', None)
            if proxy_status and proxy_status.upper() == 'ERROR':
                body = await self._read_text(response)
                message = f'status={response.status} body={body!r}'
                raise RSSProxyError(message)
            proxy_status = int(proxy_status) if proxy_status else HTTPStatus.OK.value
            content = None
            if not is_ok_status(proxy_status) or not ignore_content:
                content = await self._read_content(response)
            if not is_ok_status(proxy_status):
                return response.headers, content, url, proxy_status
            self.check_content_type(response)
        return response.headers, content, url, proxy_status

    async def read(self, url, *args, use_proxy=False, **kwargs) -> FeedResponse:
        headers = content = None
        try:
            if use_proxy:
                headers, content, url, status = await self._read_by_proxy(url, *args, **kwargs)
            else:
                headers, content, url, status = await self._read(url, *args, **kwargs)
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
            LOG.warning(type(ex).__name__ + " url=%s %s", url, ex)
        except (aiohttp.ClientResponseError, aiohttp.ContentTypeError) as ex:
            status = ex.status
        except (aiohttp.ClientError, aiohttp.InvalidURL):
            status = FeedResponseStatus.UNKNOWN_ERROR.value
        builder = FeedResponseBuilder(use_proxy=use_proxy)
        builder.url(url)
        builder.status(status)
        builder.content(content)
        builder.headers(headers)
        return builder.build()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await self.close()

    async def close(self):
        if self._close_session and self.session is not None:
            await self.session.close()
