import socket
import ssl
import asyncio
import logging
import concurrent.futures
from http import HTTPStatus

import aiodns
import aiohttp

from rssant_common import _proxy_helper
from rssant_common.helper import aiohttp_client_session
from rssant_common.dns_service import (
    DNSService, DNS_SERVICE,
    PrivateAddressError,
    NameNotResolvedError,
)

from .reader import is_webpage, is_ok_status
from .reader import (
    ContentTooLargeError,
    ContentTypeNotSupportError,
    RSSProxyError,
    FeedReaderError,
)
from .response import FeedResponse, FeedResponseStatus
from .response_builder import FeedResponseBuilder
from .useragent import DEFAULT_USER_AGENT
from . import cacert


LOG = logging.getLogger(__name__)


class AsyncFeedReader:
    def __init__(
        self,
        user_agent=DEFAULT_USER_AGENT,
        request_timeout=30,
        max_content_length=10 * 1024 * 1024,
        allow_non_webpage=False,
        proxy_url=None,
        rss_proxy_url=None,
        rss_proxy_token=None,
        dns_service: DNSService = DNS_SERVICE,
    ):
        self.resolver: aiohttp.AsyncResolver = None
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.max_content_length = max_content_length
        self.allow_non_webpage = allow_non_webpage
        self.proxy_url = proxy_url
        self.rss_proxy_url = rss_proxy_url
        self.rss_proxy_token = rss_proxy_token
        self._use_rss_proxy = self._choice_proxy()
        self.dns_service = dns_service
        self._sslcontext = ssl.create_default_context(cafile=cacert.where())

    @property
    def has_proxy(self):
        return bool(self.rss_proxy_url or self.proxy_url)

    def _choice_proxy(self) -> bool:
        return _proxy_helper.choice_proxy(
            proxy_url=self.proxy_url, rss_proxy_url=self.rss_proxy_url)

    async def _async_init(self):
        if self.resolver is None:
            loop = asyncio.get_event_loop()
            if self.dns_service is None:
                self.resolver = aiohttp.AsyncResolver(loop=loop)
            else:
                self.resolver = self.dns_service.aiohttp_resolver(loop=loop)

    def _create_session(self, proxy_url: str = None):
        return aiohttp_client_session(
            resolver=self.resolver,
            proxy_url=proxy_url,
            timeout=self.request_timeout,
        )

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
            headers=None, ignore_content=False, proxy_url=None,
    ) -> aiohttp.ClientResponse:
        headers = self._prepare_headers(
            url,
            etag=etag,
            last_modified=last_modified,
            referer=referer,
            headers=headers,
        )
        await self._async_init()
        async with self._create_session(proxy_url=proxy_url) as session:
            async with session.get(url, headers=headers, ssl=self._sslcontext) as response:
                content = None
                if not is_ok_status(response.status) or not ignore_content:
                    content = await self._read_content(response)
                if not is_ok_status(response.status):
                    return response.headers, content, url, response.status
                self.check_content_type(response)
        return response.headers, content, str(response.url), response.status

    async def _read_by_rss_proxy(
        self, url, etag=None, last_modified=None, referer=None,
        headers=None, ignore_content=False
    ):
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
        async with self._create_session() as session:
            async with session.post(self.rss_proxy_url, json=data) as response:
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

    async def _read_by_proxy(self, url, *args, **kwargs):
        if self._use_rss_proxy:
            if not self.rss_proxy_url:
                raise ValueError("rss_proxy_url not provided")
            return await self._read_by_rss_proxy(url, *args, **kwargs)
        else:
            if not self.proxy_url:
                raise ValueError("proxy_url not provided")
            return await self._read(url, *args, **kwargs, proxy_url=self.proxy_url)

    async def read(self, url, *args, use_proxy=False, **kwargs) -> FeedResponse:
        headers = content = None
        try:
            if use_proxy:
                headers, content, url, status = await self._read_by_proxy(url, *args, **kwargs)
            else:
                headers, content, url, status = await self._read(url, *args, **kwargs)
        except (socket.gaierror, aiodns.error.DNSError, NameNotResolvedError):
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
        except PrivateAddressError:
            status = FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value
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
        if self.resolver is not None:
            await self.resolver.close()
            self.resolver = None
