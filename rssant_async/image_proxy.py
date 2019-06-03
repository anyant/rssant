import logging

import aiohttp
from aiohttp.web import StreamResponse, json_response

from rssant_feedlib.reader import DEFAULT_USER_AGENT


LOG = logging.getLogger(__name__)

PROXY_REQUEST_HEADERS = [
    'Accept', 'ETag', 'If-Modified-Since'
]

PROXY_RESPONSE_HEADERS = [
    'Content-Length', 'Content-Type', 'Transfer-Encoding', 'Cache-Control', 'ETag', 'Expires',
]


async def image_proxy(request, url, referer):
    LOG.info(f'proxy image {url} referer={referer}')
    request_timeout = 30
    headers = {'User-Agent': DEFAULT_USER_AGENT, 'Referer': referer}
    for h in PROXY_REQUEST_HEADERS:
        if h in request.headers:
            headers[h] = request.headers[h]
    session = aiohttp.ClientSession(
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
        if response.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            my_response.enable_chunked_encoding()
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
