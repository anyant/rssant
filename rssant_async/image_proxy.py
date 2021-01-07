import logging
import asyncio

import aiohttp
from aiohttp.web import StreamResponse, json_response
from aiohttp import HttpVersion11

from rssant_common.dns_service import DNS_SERVICE, PrivateAddressError
from rssant_common.helper import get_referer_of_url, aiohttp_client_session
from rssant_feedlib.reader import DEFAULT_USER_AGENT
from rssant_feedlib.blacklist import compile_url_blacklist


LOG = logging.getLogger(__name__)

# 这些图片Referer错误会返回200，无法有效处理失败情况
# 需要直接设置为正确的Referer
REFERER_FORCE_URL_LIST = """
qpic.cn
qlogo.cn
qq.com
"""
is_referer_force_url = compile_url_blacklist(REFERER_FORCE_URL_LIST)


PROXY_REQUEST_HEADERS = [
    'Accept', 'Accept-Encoding',
    'ETag', 'If-Modified-Since', 'Cache-Control', 'Pragma',
]

PROXY_RESPONSE_HEADERS = [
    'Transfer-Encoding', 'Content-Encoding',
    'Cache-Control', 'ETag', 'Last-Modified', 'Expires',
    'Age', 'Pragma', 'Server', 'Date',
]


MAX_IMAGE_SIZE = int(2 * 1024 * 1024)


class ImageProxyError(Exception):
    def __init__(self, message, status=400):
        self.message = message
        self.status = status

    def to_response(self):
        return json_response({'message': self.message}, status=self.status)


_IMAGE_TIMEOUT_ERROR_S = (TimeoutError, asyncio.TimeoutError)
_IMAGE_NETWORK_ERROR_S = (
    *_IMAGE_TIMEOUT_ERROR_S,
    OSError, IOError,
    aiohttp.ClientError, asyncio.CancelledError,
)


async def get_response(session, url, headers):
    try:
        response = await session.get(url, headers=headers)
    except PrivateAddressError:
        await session.close()
        raise ImageProxyError('private address not allowed')
    except _IMAGE_NETWORK_ERROR_S as ex:
        await session.close()
        msg = '{}: {}'.format(type(ex).__name__, ex)
        LOG.info('image request failed %s, url=%r', msg, url)
        status = 504 if isinstance(ex, _IMAGE_TIMEOUT_ERROR_S) else 502
        raise ImageProxyError(msg, status=status)
    except Exception:
        await session.close()
        raise
    return response


REFERER_DENY_STATUS = {401, 403}


def _create_aiohttp_client_session():
    loop = asyncio.get_event_loop()
    resolver = DNS_SERVICE.aiohttp_resolver(loop=loop)
    request_timeout = 30
    session = aiohttp_client_session(
        resolver=resolver, timeout=request_timeout, auto_decompress=False)
    return session


def _is_chunked_response(response) -> bool:
    return response.headers.get('Transfer-Encoding', '').lower() == 'chunked'


async def image_proxy(request, url, referer=None):
    handler = ImageProxyHandler(request, url=url, referer=referer)
    return await handler.proxy()


class ImageProxyHandler:
    def __init__(self, request, url, referer=None):
        self.request = request
        self.url = url
        if not referer or is_referer_force_url(url):
            referer = get_referer_of_url(url)
        self.referer = referer
        self.session = None
        self.response = None

    async def do_cleanup(self):
        if self.response:
            self.response.close()
        if self.session:
            await self.session.close()

    async def send_proxy_request(self):
        url = self.url
        referer = self.referer
        user_agent = DEFAULT_USER_AGENT
        if callable(user_agent):
            user_agent = user_agent()
        headers = {'User-Agent': user_agent}
        for h in PROXY_REQUEST_HEADERS:
            if h in self.request.headers:
                headers[h] = self.request.headers[h]
        referer_headers = dict(headers)
        referer_headers['Referer'] = referer
        # 先尝试发带Referer的请求，不行再尝试不带Referer
        response = await get_response(self.session, url, referer_headers)
        if response.status in REFERER_DENY_STATUS:
            LOG.info(f'proxy image {url!r} referer={referer!r} '
                     f'failed {response.status}, will try without referer')
            response.close()
            response = await get_response(self.session, response.url, headers)
        is_chunked = _is_chunked_response(response)
        # using chunked encoding is forbidden for HTTP/1.0
        if is_chunked and self.request.version < HttpVersion11:
            version = 'HTTP/{0.major}.{0.minor}'.format(self.request.version)
            error_msg = f"using chunked encoding is forbidden for {version}"
            LOG.info(f'proxy image {url!r} referer={referer!r} failed: {error_msg}')
            response.close()
            raise ImageProxyError(error_msg)
        return response

    async def prepare_my_response(self):
        response = self.response
        my_response = StreamResponse(status=response.status)
        # 'Content-Length', 'Content-Type', 'Transfer-Encoding'
        if _is_chunked_response(response):
            my_response.enable_chunked_encoding()
        if response.headers.get('Content-Length'):
            content_length = int(response.headers['Content-Length'])
            if content_length > MAX_IMAGE_SIZE:
                message = 'image too large, size={}'.format(content_length)
                raise ImageProxyError(message, status=413)
            my_response.content_length = content_length
        if response.headers.get('Content-Type'):
            my_response.content_type = response.headers['Content-Type']
        for h in PROXY_RESPONSE_HEADERS:
            if h in response.headers:
                my_response.headers[h] = response.headers[h]
        await my_response.prepare(self.request)
        return my_response

    async def write_my_response(self, my_response):
        try:
            content_length = 0
            async for chunk in self.response.content.iter_chunked(8 * 1024):
                content_length += len(chunk)
                if content_length > MAX_IMAGE_SIZE:
                    LOG.warning(f'image too large, abort the response, url={self.url!r}')
                    break
                await my_response.write(chunk)
            await my_response.write_eof()
        except _IMAGE_NETWORK_ERROR_S as ex:
            msg = "image proxy failed {}: {} url={!r}".format(type(ex).__name__, ex, self.url)
            LOG.warning(msg)
        finally:
            my_response.force_close()

    async def proxy(self):
        LOG.info(f'proxy image {self.url} referer={self.referer}')
        self.session = _create_aiohttp_client_session()
        try:
            self.response = await self.send_proxy_request()
            my_response = await self.prepare_my_response()
            await self.write_my_response(my_response)
            return my_response
        except ImageProxyError as ex:
            return ex.to_response()
        finally:
            await self.do_cleanup()
