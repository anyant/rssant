import os
import json
import time
import codecs
import logging
import socket
import contextlib
from urllib.parse import urlparse, urlunparse

import aiohttp
import cchardet
from terminaltables import AsciiTable
from django.core.serializers.json import DjangoJSONEncoder


LOG = logging.getLogger(__name__)


def is_main_or_wsgi(name):
    is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
    is_wsgi = bool(os.getenv('SERVER_WSGI'))
    return name == '__main__' or is_gunicorn or is_wsgi


def pretty_format_json(data):
    """
    >>> import datetime
    >>> now = datetime.datetime.now()
    >>> assert pretty_format_json({"key": 123, "date": now})
    """
    return json.dumps(data, ensure_ascii=False, indent=4, cls=DjangoJSONEncoder)


def format_table(rows, *, header=None, border=True):
    """
    >>> assert format_table([('a', 'b', 'c'), ('d', 'e', 'f')])
    """
    table_data = []
    if header:
        table_data.append(list(header))
    elif rows:
        table_data.append([f'#{i}' for i in range(len(rows[0]))])
    table_data.extend(rows)
    table = AsciiTable(table_data)
    if not border:
        table.inner_column_border = False
        table.outer_border = False
    return table.table


def _is_encoding_exists(response):
    content_type = response.headers.get('content-type')
    return content_type and 'charset' in content_type


def detect_response_encoding(content):
    """
    >>> detect_response_encoding("你好".encode('utf-8'))
    'utf-8'
    """
    # response.apparent_encoding使用chardet检测编码，有些情况会非常慢
    # 换成cchardet实现，性能可以提升100倍
    encoding = cchardet.detect(content)['encoding']
    if encoding:
        encoding = encoding.lower()
        # 解决常见的乱码问题，chardet没检测出来基本就是windows-1254编码
        if encoding == 'windows-1254' or encoding == 'ascii':
            encoding = 'utf-8'
    else:
        encoding = 'utf-8'
    encoding = codecs.lookup(encoding).name
    return encoding


def resolve_response_encoding(response):
    if _is_encoding_exists(response) and response.encoding:
        encoding = codecs.lookup(response.encoding).name
    else:
        encoding = detect_response_encoding(response.content)
    response.encoding = encoding


async def resolve_aiohttp_response_encoding(response, content):
    if _is_encoding_exists(response) and response.charset:
        encoding = codecs.lookup(response.charset).name
    else:
        encoding = detect_response_encoding(content)
    return encoding


def coerce_url(url, default_schema='http'):
    """
    >>> coerce_url('https://blog.guyskk.com/feed.xml')
    'https://blog.guyskk.com/feed.xml'
    >>> coerce_url('blog.guyskk.com/feed.xml')
    'http://blog.guyskk.com/feed.xml'
    >>> coerce_url('feed://blog.guyskk.com/feed.xml')
    'http://blog.guyskk.com/feed.xml'
    """
    url = url.strip()
    if url.startswith("feed://"):
        return "{}://{}".format(default_schema, url[7:])
    if "://" not in url:
        return "{}://{}".format(default_schema, url)
    return url


def get_referer_of_url(url):
    schema, netloc, path, __, __, __ = urlparse(url)
    referer = urlunparse((schema, netloc, path, None, None, None))
    return referer


def aiohttp_raise_for_status(response: aiohttp.ClientResponse):
    # workaround aiohttp bug, can remove after fixed in aiohttp
    # issue: https://github.com/aio-libs/aiohttp/issues/3906
    if response.status >= 400:
        response.release()
        raise aiohttp.ClientResponseError(
            response.request_info,
            response.history,
            status=response.status,
            message=response.reason,
            headers=response.headers,
        )


def aiohttp_client_session(*, timeout=None, **kwargs):
    """use aiodns and support number timeout"""
    if isinstance(timeout, (int, float)):
        timeout = aiohttp.ClientTimeout(total=timeout)
    resolver = aiohttp.AsyncResolver()
    # Fix: No route to host. https://github.com/saghul/aiodns/issues/22
    family = socket.AF_INET
    connector = aiohttp.TCPConnector(resolver=resolver, family=family)
    return aiohttp.ClientSession(connector=connector, timeout=timeout, **kwargs)


@contextlib.contextmanager
def timer(name, response=None):
    t_begin = time.time()
    try:
        yield
    finally:
        cost = (time.time() - t_begin) * 1000
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(f'Timer X-{name}-Time: {cost:.0f}ms')
            if response:
                response[f'X-{name}-Time'] = f'{cost:.0f}ms'
