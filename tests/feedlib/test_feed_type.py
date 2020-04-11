import os.path
from pathlib import Path

import pytest

from rssant_feedlib import FeedResponseBuilder, FeedContentType


_data_dir = Path(__file__).parent / 'testdata'


def _create_builder():
    builder = FeedResponseBuilder()
    builder.url('https://blog.example.com/feed.xml')
    return builder


def _collect_feed_type_cases():
    cases = []
    for filepath in (_data_dir / 'encoding/xml').glob("*"):
        cases.append((filepath, FeedContentType.XML))
    for filepath in (_data_dir / 'encoding/chardet').glob("*.json"):
        cases.append((filepath, FeedContentType.JSON))
    for filepath in (_data_dir / 'feed_type/html').glob("*"):
        cases.append((filepath, FeedContentType.HTML))
    cases = [(os.path.relpath(x, _data_dir), t) for x, t in cases]
    return cases


@pytest.mark.parametrize('filepath, expect', _collect_feed_type_cases())
def test_feed_type(filepath, expect):
    content = Path(_data_dir / filepath).read_bytes()
    builder = _create_builder()
    builder.content(content)
    response = builder.build()
    assert response.feed_type == expect


_json_mime_types = [
    'application/ld+json',
    'application/manifest+json',
    'application/geo+json',
    'application/x-web-app-manifest+json',


]

_xml_mime_types = [
    'application/xhtml+xml',
    'application/xml',
    'application/rdf+xml',
    'application/atom+xml',
]

_html_mime_types = [
    'text/html',
    'application/html',
]

_other_mime_types = [
    'text/css',
    'application/javascript',
    'image/png',
    'application/vnd.ms-fontobject',
    'font/otf',
    'application/wasm',
    'image/bmp',
    'image/svg+xml',
    'image/x-icon',
    'text/cache-manifest',
    'text/css',
    'text/javascript',
    'text/markdown',
    'text/vcard',
    'text/calendar',
    'text/vnd.rim.location.xloc',
    'text/vtt',
    'text/x-component',
    'text/x-cross-domain-policy',
]


def _collect_mime_cases():
    cases = []
    for x in _xml_mime_types:
        cases.append((x, FeedContentType.XML))
    for x in _json_mime_types:
        cases.append((x, FeedContentType.JSON))
    for x in _html_mime_types:
        cases.append((x, FeedContentType.HTML))
    for x in _other_mime_types:
        cases.append((x, FeedContentType.OTHER))
    return cases


@pytest.mark.parametrize('mime_type, expect', _collect_mime_cases())
def test_feed_type_mime(mime_type, expect):
    builder = _create_builder()
    builder.headers({'content-type': mime_type})
    builder.content(b'hello world')
    response = builder.build()
    assert response.feed_type == expect


def test_feed_type_text_plain():
    builder = _create_builder()
    builder.headers({'content-type': 'text/plain'})
    builder.content(b'<hello>world</hello>')
    response = builder.build()
    assert response.feed_type == FeedContentType.XML
