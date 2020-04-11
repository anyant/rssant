import re
import cgi
import codecs
import typing
from http import HTTPStatus

import cchardet

from .response import FeedContentType, FeedResponse


RE_CONTENT_XML = re.compile(rb'(<\?xml|<xml|<rss|<atom|<feed|<channel)')
RE_CONTENT_HTML = re.compile(rb'(<!doctype html>|<html|<head|<body)')
RE_CONTENT_MARKUP = re.compile(rb'[<>]')

MIME_TYPE_NOT_FEED = {
    'application/octet-stream',
    'application/javascript',
    'application/vnd.',
    'text/css',
    'text/csv',
    'text/javascript',
    'image/',
    'font/',
    "audio/",
    'video/',
}


def detect_feed_type(content: bytes, mime_type: str = None) -> FeedContentType:
    """
    >>> detect_feed_type(b'<!DOCTYPE HTML>')
    <FeedContentType.HTML>
    >>> detect_feed_type(b'{"hello": "world"}')
    <FeedContentType.JSON>
    >>> detect_feed_type(b'<?xml version="1.0" encoding="utf-8"?>')
    <FeedContentType.XML>
    """
    if mime_type:
        for key in MIME_TYPE_NOT_FEED:
            if key in mime_type:
                return FeedContentType.OTHER
    head = bytes(content[:500]).strip().lower()
    if head.startswith(b'{') or head.startswith(b'['):
        return FeedContentType.JSON
    if head.startswith(b'<!doctype html>'):
        return FeedContentType.HTML
    if head.startswith(b'<?xml'):
        return FeedContentType.XML
    if RE_CONTENT_XML.search(head):
        return FeedContentType.XML
    if RE_CONTENT_HTML.search(head):
        return FeedContentType.HTML
    if mime_type:
        if 'xml' in mime_type:
            return FeedContentType.XML
        if 'html' in mime_type:
            return FeedContentType.HTML
        if 'json' in mime_type:
            return FeedContentType.JSON
    if RE_CONTENT_MARKUP.search(head):
        return FeedContentType.XML
    return FeedContentType.OTHER


# Capture the value of the XML processing instruction's encoding attribute.
# Example: <?xml version="1.0" encoding="utf-8"?>
RE_XML_ENCODING = re.compile(rb'<\?.*encoding=[\'"](.*?)[\'"].*\?>')


def _detect_xml_encoding(content: bytes) -> str:
    xml_encoding_match = RE_XML_ENCODING.search(content)
    if xml_encoding_match:
        encoding = xml_encoding_match.group(1).decode('utf-8')
        return encoding
    return None


def _detect_json_encoding(content: bytes) -> str:
    if content.startswith(b'{') or content.startswith(b'['):
        return 'utf-8'
    return None


def _detect_chardet_encoding(content: bytes) -> str:
    # chardet检测编码有些情况会非常慢，换成cchardet实现，性能可以提升100倍
    r = cchardet.detect(content)
    encoding = r['encoding'].lower()
    if r['confidence'] < 0.5:
        # 解决常见的乱码问题，chardet没检测出来基本就是iso8859-*和windows-125*编码
        if encoding.startswith('iso8859') or encoding.startswith('windows'):
            encoding = 'utf-8'
    return encoding


def _parse_content_type_header(content_type: str) -> typing.Tuple[str, str]:
    mime_type, params = cgi.parse_header(content_type)
    encoding = params.get('charset', '').replace("'", "")
    return mime_type, encoding


def _normalize_encoding(encoding: str) -> str:
    encoding = codecs.lookup(encoding).name
    if encoding == 'ascii':
        # ascii 是 utf-8 的子集，没必要用 ascii 编码
        encoding = 'utf-8'
    return encoding


class EncodingChecker:

    __slots__ = ("_content", "_encodings")

    def __init__(self, content: bytes):
        self._content = content
        self._encodings: typing.Dict[str, bool] = {}

    def _check(self, encoding: str) -> str:
        if not encoding:
            return None
        try:
            encoding = _normalize_encoding(encoding)
        except LookupError:
            return None
        ok = self._encodings.get(encoding, None)
        if ok is not None:
            return encoding if ok else None
        # https://stackoverflow.com/questions/40044517/python-decode-partial-utf-8-byte-array
        dec = codecs.getincrementaldecoder(encoding)()
        try:
            dec.decode(self._content)
        except UnicodeDecodeError:
            self._encodings[encoding] = False
            return None
        self._encodings[encoding] = True
        return encoding

    def check(self, encoding: str) -> str:
        encoding = self._check(encoding)
        if not encoding:
            return encoding
        # Since ISO-8859-1 is a 1 byte per character encoding, it will always work.
        if '8859' in encoding or 'latin' in encoding:
            if self._check('utf-8'):
                return 'utf-8'
        return encoding


def detect_content_encoding(content: bytes, http_encoding: str = None):
    """
    >>> detect_content_encoding(b'hello', 'text/xml;charset=utf-8')
    'utf-8'
    >>> detect_content_encoding(b'hello', 'text/xml;charset=unknown')
    'utf-8'
    >>> content = '<?xml version="1.0" encoding="utf-8"?>'.encode('utf-8')
    >>> detect_content_encoding(content)
    'utf-8'
    >>> detect_content_encoding("你好".encode('utf-8'))
    'utf-8'
    """
    content = bytes(content[:2000])  # only need peek partial content
    checker = EncodingChecker(content)
    if http_encoding:
        encoding = checker.check(http_encoding)
        if encoding is not None:
            return encoding
    encoding = checker.check(_detect_json_encoding(content))
    if encoding is not None:
        return encoding
    encoding = checker.check(_detect_xml_encoding(content))
    if encoding is not None:
        return encoding
    encoding = checker.check(_detect_chardet_encoding(content))
    if encoding is not None:
        return encoding
    return 'utf-8'


class FeedResponseBuilder:

    __slots__ = (
        '_content',
        '_status',
        '_url',
        '_headers',
        '_use_proxy',
    )

    def __init__(self, *, use_proxy=False):
        self._content = None
        self._status = None
        self._url = None
        self._headers = None
        self._use_proxy = use_proxy

    def content(self, value: bytes):
        self._content = value

    def status(self, value: str):
        self._status = value

    def url(self, value: str):
        self._url = value

    def headers(self, headers: dict):
        self._headers = headers

    def build(self) -> FeedResponse:
        mime_type = feed_type = encoding = http_encoding = None
        if self._headers:
            content_type_header = self._headers.get('content-type')
            if content_type_header:
                mime_type, http_encoding = _parse_content_type_header(content_type_header)
        if self._content:
            feed_type = detect_feed_type(self._content, mime_type)
            encoding = detect_content_encoding(self._content, http_encoding)
        etag = last_modified = None
        if self._headers:
            etag = self._headers.get("etag")
            last_modified = self._headers.get("last-modified")
        status = self._status if self._status is not None else HTTPStatus.OK.value
        return FeedResponse(
            content=self._content,
            status=status,
            url=self._url,
            etag=etag,
            last_modified=last_modified,
            encoding=encoding,
            mime_type=mime_type,
            feed_type=feed_type,
            use_proxy=self._use_proxy,
        )
