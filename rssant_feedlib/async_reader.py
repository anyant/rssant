import socket
import ssl
import asyncio
import concurrent.futures
import ipaddress
from urllib.parse import urlparse

import aiodns
import aiohttp

from rssant_common.helper import resolve_aiohttp_response_encoding

from .reader import DEFAULT_USER_AGENT, FeedResponseStatus, PrivateAddressError, ContentTooLargeError


class AsyncFeedReader:
    def __init__(
        self,
        session=None,
        user_agent=DEFAULT_USER_AGENT,
        request_timeout=30,
        max_content_length=10 * 1024 * 1024,
        allow_private_address=False,
    ):
        self._close_session = session is None
        self.session = session
        self.resolver = None
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.max_content_length = max_content_length
        self.allow_private_address = allow_private_address

    async def _async_init(self):
        if self.resolver is None:
            self.resolver = aiodns.DNSResolver(loop=asyncio.get_event_loop())
        if self.session is None:
            self.session = aiohttp.ClientSession(
                read_timeout=self.request_timeout,
                conn_timeout=self.request_timeout,
            )

    async def _resolve_hostname(self, hostname):
        addrinfo = await self.resolver.gethostbyname(hostname, socket.AF_INET)
        # https://pycares.readthedocs.io/en/latest/channel.html#pycares.Channel.query
        # extra type ares_host_result: addresses, aliases, name
        if getattr(addrinfo, 'addresses', None):
            for ip in addrinfo.addresses:
                yield ip
        elif getattr(addrinfo, 'host'):
            yield addrinfo.host

    async def check_private_address(self, url):
        """Prevent request private address, which will attack local network"""
        await self._async_init()
        hostname = urlparse(url).hostname
        async for ip in self._resolve_hostname(hostname):
            ip = ipaddress.ip_address(ip)
            if ip.is_private:
                raise PrivateAddressError(ip)

    async def _read_content(self, response):
        content_length = response.headers.get('Content-Length')
        if content_length:
            content_length = int(content_length)
            if content_length > self.max_content_length:
                msg = 'Content length {} larger than limit {}'.format(
                    content_length, self.max_content_length)
                raise ContentTooLargeError(msg)
        content_length = 0
        content = []
        async for chunk in response.content.iter_chunked(8 * 1024):
            content_length += len(chunk)
            if content_length > self.max_content_length:
                msg = 'Content length larger than limit {}'.format(self.max_content_length)
                raise ContentTooLargeError(msg)
            content.append(chunk)
        content = b''.join(content)
        response._body = content
        encoding = await resolve_aiohttp_response_encoding(response, content)
        text = content.decode(encoding)
        response.rssant_encoding = encoding
        response.rssant_content = content
        response.rssant_text = text

    async def _read(self, url, etag=None, last_modified=None, referer=None, headers=None, ignore_content=False):
        if headers is None:
            headers = {}
        headers['User-Agent'] = self.user_agent
        if etag:
            headers["ETag"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        if referer:
            headers["Referer"] = referer
        await self._async_init()
        if not self.allow_private_address:
            await self.check_private_address(url)
        async with self.session.get(url, headers=headers) as response:
            response.raise_for_status()
            if not ignore_content:
                await self._read_content(response)
        return response

    async def read(self, *args, **kwargs):
        response = None
        try:
            response = await self._read(*args, **kwargs)
        except (socket.gaierror, aiodns.error.DNSError):
            status = FeedResponseStatus.DNS_ERROR.value
        except PrivateAddressError:
            status = FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value
        except (socket.timeout, TimeoutError, aiohttp.ServerTimeoutError,
                asyncio.TimeoutError, concurrent.futures.TimeoutError):
            status = FeedResponseStatus.CONNECTION_TIMEOUT.value
        except (ssl.SSLError, ssl.CertificateError,
                aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch):
            status = FeedResponseStatus.SSL_ERROR.value
        except aiohttp.ClientProxyConnectionError:
            status = FeedResponseStatus.PROXY_ERROR.value
        except (ConnectionError, aiohttp.ServerDisconnectedError,
                aiohttp.ServerConnectionError):
            status = FeedResponseStatus.CONNECTION_RESET.value
        except aiohttp.ClientPayloadError:
            status = FeedResponseStatus.CHUNKED_ENCODING_ERROR.value
        except UnicodeDecodeError:
            status = FeedResponseStatus.CONTENT_DECODING_ERROR.value
        except ContentTooLargeError:
            status = FeedResponseStatus.CONTENT_TOO_LARGE_ERROR.value
        except aiohttp.ClientResponseError as ex:
            status = ex.status
            if ex.history:
                response = ex.history[-1]
        except aiohttp.ClientError:
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
