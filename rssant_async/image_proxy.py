import logging

import yarl
import aiohttp
from aiohttp.web import StreamResponse, json_response
from aiohttp import HttpVersion11

from rssant_config import CONFIG
from rssant_common.helper import get_referer_of_url, aiohttp_client_session
from rssant_feedlib.reader import DEFAULT_USER_AGENT, PrivateAddressError
from rssant_feedlib.async_reader import AsyncFeedReader
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
    'Accept', 'Accept-Encoding', 'ETag', 'If-Modified-Since', 'Cache-Control',
]

PROXY_RESPONSE_HEADERS = [
    'Transfer-Encoding', 'Cache-Control', 'ETag', 'Last-Modified', 'Expires',
]


MAX_IMAGE_SIZE = int(2 * 1024 * 1024)


class ImageProxyError(Exception):
    def __init__(self, message, status=400):
        self.message = message
        self.status = status

    def to_response(self):
        return json_response({'message': self.message}, status=self.status)


async def check_private_address(url):
    if CONFIG.allow_private_address:
        return
    async with AsyncFeedReader() as reader:
        try:
            await reader.check_private_address(url)
        except PrivateAddressError:
            raise ImageProxyError('private address not allowed')


async def get_response(session, url, headers):
    try:
        response = await session.get(url, headers=headers)
    except (OSError, TimeoutError, IOError, aiohttp.ClientError) as ex:
        await session.close()
        raise ImageProxyError(str(ex))
    except Exception:
        await session.close()
        raise
    if yarl.URL(response.url) != yarl.URL(url):
        try:
            await check_private_address(str(response.url))
        except Exception:
            await session.close()
            raise
    return response


REFERER_DENY_STATUS = {401, 403}


async def image_proxy(request, url, referer=None):
    if not referer or is_referer_force_url(url):
        referer = get_referer_of_url(url)
    LOG.info(f'proxy image {url} referer={referer}')
    session = response = None

    async def do_cleanup():
        nonlocal session, response
        if response:
            response.close()
        if session:
            await session.close()

    try:
        await check_private_address(url)
        headers = {'User-Agent': DEFAULT_USER_AGENT}
        for h in PROXY_REQUEST_HEADERS:
            if h in request.headers:
                headers[h] = request.headers[h]
        referer_headers = dict(headers)
        referer_headers['Referer'] = referer
        request_timeout = 30
        session = aiohttp_client_session(
            auto_decompress=False,
            timeout=request_timeout,
        )
        # 先尝试发带Referer的请求，不行再尝试不带Referer
        response = await get_response(session, url, referer_headers)
        if response.status in REFERER_DENY_STATUS:
            LOG.info(f'proxy image {url} referer={referer} '
                     f'failed {response.status}, will try without referer')
            response.close()
            response = await get_response(session, response.url, headers)
        is_chunked = response.headers.get('Transfer-Encoding', '').lower() == 'chunked'
        # using chunked encoding is forbidden for HTTP/1.0
        if is_chunked and request.version < HttpVersion11:
            version = 'HTTP/{0.major}.{0.minor}'.format(request.version)
            error_msg = f"using chunked encoding is forbidden for {version}"
            LOG.info(f'proxy image {url} referer={referer} failed: {error_msg}')
            response.close()
            raise ImageProxyError(error_msg)
    except ImageProxyError as ex:
        await do_cleanup()
        return ex.to_response()
    except Exception:
        await do_cleanup()
        raise
    try:
        my_response = StreamResponse(status=response.status)
        # 'Content-Length', 'Content-Type', 'Transfer-Encoding'
        if is_chunked:
            my_response.enable_chunked_encoding()
        elif response.headers.get('Transfer-Encoding'):
            my_response.headers['Transfer-Encoding'] = response.headers['Transfer-Encoding']
        if response.headers.get('Content-Length'):
            content_length = int(response.headers['Content-Length'])
            if content_length > MAX_IMAGE_SIZE:
                return json_response({'message': 'image too large'}, status=413)
            my_response.content_length = content_length
        if response.headers.get('Content-Type'):
            my_response.content_type = response.headers['Content-Type']
        for h in PROXY_RESPONSE_HEADERS:
            if h in response.headers:
                my_response.headers[h] = response.headers[h]
        await my_response.prepare(request)
    except Exception:
        await do_cleanup()
        raise
    try:
        content_length = 0
        async for chunk in response.content.iter_chunked(8 * 1024):
            content_length += len(chunk)
            if content_length > MAX_IMAGE_SIZE:
                LOG.warning(f'image too large, abort the response, url={url}')
                my_response.force_close()
                break
            await my_response.write(chunk)
        await my_response.write_eof()
    finally:
        await do_cleanup()
    return my_response
