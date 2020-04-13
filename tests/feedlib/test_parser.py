import logging
import os
from pathlib import Path

import pytest

from rssant_feedlib import (
    RawFeedParser, FeedParser, FeedParserError, FeedResponseBuilder,
)
from rssant_feedlib.raw_parser import _MAX_CONTENT_LENGTH as _RAW_MAX_CONTENT_LENGTH
from rssant_feedlib.raw_parser import _MAX_SUMMARY_LENGTH as _RAW_MAX_SUMMARY_LENGTH
from rssant_feedlib.parser import _MAX_CONTENT_LENGTH
from rssant_feedlib.parser import _MAX_SUMMARY_LENGTH


LOG = logging.getLogger(__name__)


_data_dir = Path(__file__).parent / 'testdata/parser'


def _collect_filenames(base_dir):
    names = [x.name for x in Path(base_dir).glob('*')]
    return names


def _read_response(base_dir, filename):
    content = (Path(base_dir) / filename).read_bytes()
    response = _create_builder(content).build()
    return response


def _create_builder(content=None):
    builder = FeedResponseBuilder()
    builder.url('https://blog.example.com/feed')
    if content is not None:
        builder.content(content)
    return builder


@pytest.mark.parametrize('filename', _collect_filenames(_data_dir / 'well'))
def test_raw_parse_well(filename):
    response = _read_response(_data_dir / 'well', filename)
    parser = RawFeedParser()
    result = parser.parse(response)
    assert result
    assert not result.warnings and not isinstance(result.warnings, str)
    assert result.storys
    assert result.feed['version']
    assert result.feed['title']


@pytest.mark.parametrize('filename', _collect_filenames(_data_dir / 'warn'))
def test_raw_parse_warn(filename):
    response = _read_response(_data_dir / 'warn', filename)
    parser = RawFeedParser()
    result = parser.parse(response)
    assert result
    assert result.warnings and isinstance(result.warnings, list)
    assert result.storys
    assert result.feed['version']
    assert result.feed['title']


@pytest.mark.parametrize('filename', _collect_filenames(_data_dir / 'failed'))
def test_raw_parse_failed(filename):
    response = _read_response(_data_dir / 'failed', filename)
    parser = RawFeedParser()
    with pytest.raises(FeedParserError) as ex:
        parser.parse(response)
    assert ex


def test_raw_parse_bad_encoding():
    content = os.urandom(16 * 1024)
    builder = FeedResponseBuilder()
    builder.url('https://blog.example.com/feed')
    builder.content(content)
    response = builder.build()
    parser = RawFeedParser()
    with pytest.raises(FeedParserError) as ex:
        parser.parse(response)
    assert ex


def test_parse_story_no_summary():
    filename = 'well/v2ex-no-summary.xml'
    response = _read_response(_data_dir, filename)
    raw_result = RawFeedParser().parse(response)
    assert raw_result.storys
    assert not raw_result.storys[0]['summary']
    result = FeedParser().parse(raw_result)
    assert result.storys
    assert len(raw_result.storys) == len(result.storys)
    assert result.storys[0]['summary']


large_json_feed = """
{
    "version": "https://jsonfeed.org/version/1",
    "title": "My Example Feed",
    "home_page_url": "https://example.org/",
    "feed_url": "https://example.org/feed.json",
    "items": [
        {
            "id": "1",
            "content_html": "${content}",
            "summary": "${summary}",
            "url": "https://example.org/initial-post"
        }
    ]
}
"""

large_xml_feed = """
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">

  <title>Example Feed</title>
  <link href="http://example.org/"/>
  <updated>2003-12-13T18:30:02Z</updated>
  <author>
    <name>John Doe</name>
  </author>
  <id>urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6</id>

  <entry>
    <title>Atom-Powered Robots Run Amok</title>
    <link href="http://example.org/2003/12/13/atom03"/>
    <id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
    <updated>2003-12-13T18:30:02Z</updated>
    <summary><![CDATA[${summary}]]></summary>
    <content><![CDATA[${content}]]></content>
  </entry>

</feed>
"""

large_feed_templates = {
    'xml': large_xml_feed,
    'json': large_json_feed,
}


@pytest.mark.parametrize('template_name', ['json', 'xml'])
@pytest.mark.parametrize('content_length, summary_length', [
    (0, 0),
    (0, _MAX_SUMMARY_LENGTH),
    (_MAX_CONTENT_LENGTH, 0),
    (0, _RAW_MAX_SUMMARY_LENGTH + 100),
    (_RAW_MAX_CONTENT_LENGTH + 100, 0),
    (_RAW_MAX_CONTENT_LENGTH * 3, _RAW_MAX_SUMMARY_LENGTH * 3),
])
def test_parse_large_content(template_name, content_length, summary_length):
    content_snip = "<span>12345678</span>"
    summary_snip = '<span>123</span>'
    content_repeat = (content_length // len(content_snip)) + 1
    content = content_snip * content_repeat
    summary_repeat = (summary_length // len(summary_snip)) + 1
    summary = summary_snip * summary_repeat
    template = large_feed_templates[template_name]
    # use replace instead format to avoid KeyError for json string
    data = template\
        .replace('${content}', content)\
        .replace('${summary}', summary)\
        .encode('utf-8')
    response = _create_builder(content=data).build()
    raw_result = RawFeedParser().parse(response)
    assert raw_result and len(raw_result.storys) == 1
    assert len(raw_result.storys[0]['content']) <= _RAW_MAX_CONTENT_LENGTH
    assert len(raw_result.storys[0]['summary']) <= _RAW_MAX_SUMMARY_LENGTH
    result = FeedParser().parse(raw_result)
    assert result and len(result.storys) == 1
    assert len(result.storys[0]['content']) <= _MAX_CONTENT_LENGTH
    assert len(result.storys[0]['summary']) <= _MAX_SUMMARY_LENGTH


def _collect_parser_cases():
    cases = []
    for base_dir in ['well', 'warn']:
        for filename in _collect_filenames(_data_dir / base_dir):
            cases.append(base_dir + '/' + filename)
    return cases


@pytest.mark.parametrize('filepath', _collect_parser_cases())
def test_parser_and_checksum(filepath):
    response = _read_response(_data_dir, filepath)
    raw_parser = RawFeedParser()
    raw_result = raw_parser.parse(response)
    assert raw_result.feed
    assert raw_result.storys
    parser = FeedParser()
    result = parser.parse(raw_result)
    assert result.feed
    assert result.storys
    assert result.checksum.size() == len(result.storys)
