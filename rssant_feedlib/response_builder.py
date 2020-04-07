import re
import cgi
import codecs
import typing
from http import HTTPStatus

import cchardet

from .response import FeedContentType, FeedResponse


RE_CONTENT_XML = re.compile(rb'(<\?xml|<xml|<rss|<atom|<feed|<channel)')
RE_CONTENT_HTML = re.compile(rb'(<!doctype html>|<html|<head|<body)')


def detect_content_type(content: bytes, content_type_header: str = None) -> FeedContentType:
    """
    >>> detect_content_type(b'<!DOCTYPE HTML>')
    <FeedContentType.HTML>
    >>> detect_content_type(b'{"hello": "world"}')
    <FeedContentType.JSON>
    >>> detect_content_type(b'<?xml version="1.0" encoding="utf-8"?>')
    <FeedContentType.XML>
    """
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
    return FeedContentType.XML


# Capture the value of the XML processing instruction's encoding attribute.
# Example: <?xml version="1.0" encoding="utf-8"?>
RE_XML_ENCODING = re.compile(rb'<\?.*encoding=[\'"](.*?)[\'"].*\?>')


def _detect_xml_encoding(content: bytes) -> str:
    xml_encoding_match = RE_XML_ENCODING.search(content)
    if xml_encoding_match:
        encoding = xml_encoding_match.group(1).decode('utf-8')
        return encoding
    return None


def _detect_chardet_encoding(content: bytes) -> str:
    # chardet检测编码有些情况会非常慢，换成cchardet实现，性能可以提升100倍
    r = cchardet.detect(content)
    encoding = r['encoding'].lower()
    if r['confidence'] < 0.5:
        # 解决常见的乱码问题，chardet没检测出来基本就是iso8859-*和windows-125*编码
        if encoding.startswith('iso8859') or encoding.startswith('windows'):
            encoding = 'utf-8'
    elif encoding == 'ascii':
        # ascii 是 utf-8 的子集，没必要用 ascii 编码
        encoding = 'utf-8'
    return encoding


def _detect_http_encoding(content_type: str) -> str:
    _, params = cgi.parse_header(content_type)
    encoding = params.get('charset', '').replace("'", "")
    return encoding


def _normalize_encoding(encoding: str) -> str:
    return codecs.lookup(encoding).name


class EncodingChecker:

    __slots__ = ("_content", "_encodings")

    def __init__(self, content: bytes):
        self._content = content
        self._encodings: typing.Dict[str, bool] = {}

    def check(self, encoding: str) -> str:
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


def detect_content_encoding(content: bytes, content_type_header: str = None):
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
    if content_type_header:
        encoding = checker.check(_detect_http_encoding(content_type_header))
        if encoding is not None:
            return encoding
    encoding = checker.check(_detect_xml_encoding(content))
    if encoding is not None:
        return encoding
    encoding = checker.check(_detect_chardet_encoding(content))
    if encoding is not None:
        return encoding
    if checker.check('utf-8'):
        return 'utf-8'
    return None


class FeedResponseBuilder:

    __slots__ = (
        '_content',
        '_status',
        '_url',
        '_headers',
    )

    def __init__(self):
        self._content = None
        self._status = None
        self._url = None
        self._headers = None

    def content(self, value: bytes):
        self._content = value

    def status(self, value: str):
        self._status = value

    def url(self, value: str):
        self._url = value

    def headers(self, headers: dict):
        self._headers = headers

    def build(self) -> FeedResponse:
        content_type = encoding = None
        if self._content and self._headers:
            content_type_header = self._headers.get('content-type')
            content_type = detect_content_type(self._content, content_type_header)
            encoding = detect_content_encoding(self._content, content_type_header)
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
            content_type=content_type
        )
