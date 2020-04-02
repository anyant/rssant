import cgi
from io import BytesIO

import feedparser
from validr import mark_index

from .schema import validate_feed, validate_story


feedparser.RESOLVE_RELATIVE_URIS = False
feedparser.SANITIZE_HTML = False


class FeedParserResult:
    def __init__(self, feed, entries, version, bozo, bozo_exception):
        self.feed = feed
        self.entries = entries
        self.version = version
        self.bozo = bozo
        self.bozo_exception = bozo_exception
        self.response = None
        self.use_proxy = False


def _process_response(response):
    if response.encoding == 'utf-8':
        content = response.content
    else:
        content = response.text.encode('utf-8')
    headers = _process_headers(response.headers, url=response.url)
    return content, headers


def _process_headers(self, headers=None, url=None):
    if headers is None:
        headers = {}
    headers = {k.lower(): v for k, v in headers.items()}
    content_type = headers.get('content-type', '')
    if content_type:
        mime_type, __ = cgi.parse_header(content_type)
    else:
        mime_type = 'application/xml'
    headers['content-type'] = f'{mime_type};charset=utf-8'
    headers.pop('content-encoding', None)
    headers.pop('transfer-encoding', None)
    if url:
        headers['content-location'] = url
    return headers


def _parse(content, headers, validate=True):
    """解析Feed，返回结果可以pickle序列化，便于多进程中使用"""
    stream = BytesIO(content)
    feed = feedparser.parse(
        stream, response_headers=headers,
    )
    bozo = feed.bozo
    if not feed.bozo:
        # 没有title的feed视为错误
        title = feed.feed.get("title")
        if not title:
            bozo = 1
            bozo_exception = "the feed no title, considered not a feed."
        else:
            bozo = 0
            bozo_exception = ""
    else:
        bozo = feed.bozo
        ex = feed.get("bozo_exception")
        if not ex:
            bozo_exception = ""
        else:
            name = type(ex).__module__ + "." + type(ex).__name__
            bozo_exception = f"{name}: {ex}"
    if validate:
        feed_info = validate_feed(feed.feed)
        entries = []
        for i, x in enumerate(feed.entries):
            with mark_index(i):
                entries.append(validate_story(x))
    else:
        feed_info = feed.feed
        entries = feed.entries
    version = feed.get("version") or ""
    result = FeedParserResult(
        feed=feed_info,
        entries=entries,
        version=version,
        bozo=bozo,
        bozo_exception=bozo_exception,
    )
    return result


class FeedParser:

    @staticmethod
    def parse(content, headers=None, url=None, validate=True):
        """解析Feed

        Args:
            content (bytes): UTF-8编码的内容
            headers (dict): HTTP响应头
            url (str): 来源URL
        """
        headers = _process_headers(headers, url=url)
        return _parse(content, headers, validate=validate)

    @staticmethod
    def parse_response(response, validate=True):
        """从requests.Response解析Feed"""
        content, headers = _process_response(response)
        result = _parse(content, headers, validate=validate)
        result.response = response
        return result
