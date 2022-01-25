import codecs
import re
from pathlib import Path

import pytest

from rssant_feedlib import FeedResponseBuilder
from rssant_feedlib.response_builder import detect_content_encoding

_data_dir = Path(__file__).parent / 'testdata/encoding'


def _normalize_encoding(x):
    return codecs.lookup(x).name


def _create_builder():
    builder = FeedResponseBuilder()
    builder.url('https://blog.example.com/feed.xml')
    return builder


def _collect_chardet_cases():
    cases = []
    for filepath in (_data_dir / 'chardet').glob("*"):
        encodings = filepath.name.split('.')[0].split(':')
        encodings = [_normalize_encoding(x) for x in encodings]
        cases.append((filepath.name, encodings))
    return cases


@pytest.mark.parametrize("filename, expects", _collect_chardet_cases())
def test_chardet(filename, expects):
    filepath = _data_dir / 'chardet' / filename
    content = filepath.read_bytes()
    builder = _create_builder()
    builder.content(content)
    response = builder.build()
    assert response.encoding in expects


def _collect_header_cases():
    return [
        ('utf-8', "application/json;charset=utf-8"),
        ('utf-8', "application/atom+xml; charset='us-ascii'"),
        ('gb2312', "application/atom+xml; charset='gb2312'"),
        ('gbk', "application/atom+xml;CHARSET=GBK"),
    ]


@pytest.mark.parametrize("expect, header", _collect_header_cases())
def test_header(expect, header):
    expect = _normalize_encoding(expect)
    content = b'hello world'

    builder = _create_builder()
    builder.headers({'content-type': header})
    builder.content(content)
    response = builder.build()

    assert response.encoding == expect


def _collect_xml_cases():
    cases = []
    for filepath in (_data_dir / 'xml').glob("*"):
        content = filepath.read_bytes()
        match = re.search(rb'encoding=[\'"](.*?)["\']', content)
        if match:
            expect = _normalize_encoding(match.group(1).decode())
        else:
            expect = 'utf-8'
        cases.append((filepath.name, expect))
    return cases


@pytest.mark.parametrize("filename, expect", _collect_xml_cases())
def test_xml(filename, expect):
    filepath = _data_dir / 'xml' / filename
    content = filepath.read_bytes()
    builder = _create_builder()
    builder.content(content)
    response = builder.build()
    # when declaration is iso8859 but content is ascii, detect as utf-8 is fine
    if expect == 'iso8859-1' and response.encoding == 'utf-8':
        assert content.decode(response.encoding)
    else:
        assert response.encoding == expect


def _collect_mixed_cases():
    xml_filenames = [
        'http_application_atom_xml_charset_overrides_encoding.xml',
        'http_application_rss_xml_charset_overrides_encoding.xml',
    ]
    headers = [
        "text/xml;",
        "application/atom+xml; charset='iso8859-1'",
    ]
    expect = 'utf-8'
    cases = []
    for filename in xml_filenames:
        for header in headers:
            cases.append((filename, header, expect))
    return cases


@pytest.mark.parametrize("filename, header, expect", _collect_mixed_cases())
def test_mixed(filename, header, expect):
    filepath = _data_dir / 'mixed' / filename
    content = filepath.read_bytes()
    builder = _create_builder()
    builder.content(content)
    builder.headers({'content-type': header})
    response = builder.build()
    assert response.encoding == expect


def test_messy_encoding():
    text = '[Netflix]晴雅集/阴阳师(上) Dream of Eternity.HD1080P.国语中字'
    content = text.encode('utf-8')
    for i in range(8, len(content)):
        assert detect_content_encoding(content[:i]) == 'utf-8'
