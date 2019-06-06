import logging

import aiohttp
from aiohttp.web import StreamResponse, json_response

from rssant_feedlib.reader import DEFAULT_USER_AGENT, PrivateAddressError
from rssant_feedlib.async_reader import AsyncFeedReader


LOG = logging.getLogger(__name__)

PROXY_REQUEST_HEADERS = [
    'Accept', 'Accept-Encoding', 'ETag', 'If-Modified-Since', 'Cache-Control',
]

PROXY_RESPONSE_HEADERS = [
    'Transfer-Encoding', 'Cache-Control', 'ETag', 'Last-Modified', 'Expires',
]


async def image_proxy(request, url, referer):
    LOG.info(f'proxy image {url} referer={referer}')
    async with AsyncFeedReader() as reader:
        try:
            await reader.check_private_address(url)
        except PrivateAddressError:
            my_response = json_response({'message': 'private address not allowed'}, status=400)
            return my_response
    request_timeout = 30
    headers = {'User-Agent': DEFAULT_USER_AGENT, 'Referer': referer}
    for h in PROXY_REQUEST_HEADERS:
        if h in request.headers:
            headers[h] = request.headers[h]
    session = aiohttp.ClientSession(
        auto_decompress=False,
        read_timeout=request_timeout,
        conn_timeout=request_timeout,
    )
    try:
        response = await session.get(url, headers=headers)
    except (OSError, TimeoutError, IOError, aiohttp.ClientError) as ex:
        await session.close()
        my_response = json_response({'message': str(ex)}, status=400)
        return my_response
    except Exception:
        await session.close()
        raise
    try:
        my_response = StreamResponse(status=response.status)
        # 'Content-Length', 'Content-Type', 'Transfer-Encoding'
        if response.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            my_response.enable_chunked_encoding()
        elif response.headers.get('Transfer-Encoding'):
            my_response.headers['Transfer-Encoding'] = response.headers['Transfer-Encoding']
        if response.headers.get('Content-Length'):
            my_response.content_length = int(response.headers['Content-Length'])
        if response.headers.get('Content-Type'):
            my_response.content_type = response.headers['Content-Type']
        for h in PROXY_RESPONSE_HEADERS:
            if h in response.headers:
                my_response.headers[h] = response.headers[h]
        await my_response.prepare(request)
    except Exception:
        response.close()
        await session.close()
        raise
    try:
        async for chunk in response.content.iter_chunked(8 * 1024):
            await my_response.write(chunk)
        await my_response.write_eof()
    finally:
        response.close()
        await session.close()
    return my_response
