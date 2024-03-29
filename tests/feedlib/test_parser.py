import logging
import os
import re
import json
import datetime
from pathlib import Path

import pytest

from rssant_feedlib import (
    RawFeedParser, FeedParser, FeedResult,
    FeedParserError, FeedResponseBuilder,
)
from rssant_feedlib.raw_parser import _MAX_CONTENT_LENGTH as _RAW_MAX_CONTENT_LENGTH
from rssant_feedlib.raw_parser import _MAX_SUMMARY_LENGTH as _RAW_MAX_SUMMARY_LENGTH
from rssant_feedlib.parser import _MAX_CONTENT_LENGTH
from rssant_feedlib.parser import _MAX_SUMMARY_LENGTH
from rssant_feedlib.parser import _MAX_STORYS


LOG = logging.getLogger(__name__)

UTC = datetime.timezone.utc

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
    response = _create_builder(content=content).build()
    parser = RawFeedParser()
    with pytest.raises(FeedParserError) as ex:
        parser.parse(response)
    assert ex


def test_raw_parser_incomplete_content():
    response = _read_response(_data_dir / 'warn', 'https-tmioe-com-feed.xml')
    parser = RawFeedParser()
    result = parser.parse(response)
    assert len(result.storys) == 5
    assert result.feed['version'] == 'rss20'
    assert result.feed['title'] == 'ZAPRO · 杂铺'
    expect_title = "TikTok 抖音国际版 v18.6.2 解锁全部国家任意切换"
    expect_url = "https://tmioe.com/1463.html"
    got_storys = [x for x in result.storys if x['url'] == expect_url]
    assert got_storys
    assert got_storys[0]['title'] == expect_title


def test_raw_parse_date_timestamp():
    content = '''
    <rss version="2.0">
    <channel>
    <title>博客中国</title>
    <generator>http://www.blogchina.com</generator>
    <item>
        <title><![CDATA[ 新能源车崛起，传统汽车如何避免诺基亚式危机？ ]]></title>
        <link>http://jianghulaoliu.blogchina.com/956463775.html</link>
        <description><![CDATA[ 近日，特斯拉降价16万使得特斯拉消费激增 ]]></description>
        <source></source>
        <pubDate>1611146768</pubDate>
    </item>
    '''.encode('utf-8')
    response = _create_builder(content=content).build()
    result = RawFeedParser().parse(response)
    assert len(result.storys) == 1
    dt: datetime.datetime = result.storys[0]['dt_published']
    assert dt == datetime.datetime.fromtimestamp(1611146768, tz=UTC)


def test_parse_story_no_id_no_summary_no_url():
    # total 3 storys
    # skip the no id story
    # story#0 no content no summary, no url
    # story#1 has content no summary, no url but id is valid url
    filename = 'warn/v2ex-no-id-no-summary-no-url.xml'
    response = _read_response(_data_dir, filename)

    raw_result = RawFeedParser().parse(response)
    assert raw_result.storys
    # assert skip the no id story
    assert len(raw_result.storys) == 2
    # assert no summary
    assert not raw_result.storys[0]['summary']
    assert not raw_result.storys[1]['summary']
    # assert content
    assert not raw_result.storys[0]['content']
    assert raw_result.storys[1]['content']
    # assert pick id as url, discard the invalid one
    assert not raw_result.storys[0]['url']
    assert raw_result.storys[1]['url']

    result = FeedParser().parse(raw_result)
    assert result.storys
    assert len(raw_result.storys) == len(result.storys)
    # assert content
    assert not result.storys[0]['content']
    assert result.storys[1]['content']
    # assert extract summary from content
    assert not result.storys[0]['summary']
    assert result.storys[1]['summary']
    # assert pick id as url, discard the invalid one
    assert not result.storys[0]['url']
    assert result.storys[1]['url']


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


def test_parse_too_many_storys():
    items = []
    num_storys = 2000
    base = datetime.datetime.now()
    for i in range(num_storys):
        if i < num_storys // 2:
            date_published = None
        else:
            date_published = (base + datetime.timedelta(seconds=i)).isoformat()
        items.append({
            "id": f"{i}",
            "content_html": f"content_{i}",
            "summary": f"summary_{i}",
            "url": f"https://example.org/post/{i}",
            "date_published": date_published,
        })
    feed = {
        "version": "https://jsonfeed.org/version/1",
        "title": "Too many storys",
        "home_page_url": "https://example.org/",
        "feed_url": "https://example.org/feed.json",
        "items": items
    }
    data = json.dumps(feed).encode('utf-8')
    response = _create_builder(data).build()
    raw_result = RawFeedParser().parse(response)
    assert len(raw_result.storys) == num_storys
    result = FeedParser().parse(raw_result)
    assert len(result.storys) == _MAX_STORYS
    expected = set(range(num_storys - _MAX_STORYS, num_storys))
    story_ids = {int(x['ident'].split('::')[0]) for x in result.storys}
    assert story_ids == expected


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


def _parse_well_feed(filename) -> FeedResult:
    response = _read_response(_data_dir / 'well', filename)
    raw_result = RawFeedParser().parse(response)
    assert raw_result.feed
    assert raw_result.storys
    assert not raw_result.warnings
    result = FeedParser().parse(raw_result)
    assert len(result.storys) == len(raw_result.storys)
    return result


def test_parser_iframe():
    result = _parse_well_feed('bilibili_iframe.xml')
    expect = r'https://player\.bilibili\.com/player\.html\?aid=\d+'
    for story in result.storys:
        assert story['iframe_url']
        assert re.match(expect, story['iframe_url'])


def test_parser_iframe_link():
    result = _parse_well_feed('dapenti_blog_iframe.xml')
    expect = r'https://video.h5.weibo.cn/1034:4671653473616001/4671655702434113'
    assert len(result.storys) == 30
    story = result.storys[1]
    assert story['iframe_url'] == expect


def test_parser_audio_typlog():
    result = _parse_well_feed('typlog_audio.xml')
    expect = r'https://chtbl\.com/track/6AGABB/r\.typlog\.com/.+\.mp3'
    for story in result.storys:
        assert story['audio_url']
        assert re.match(expect, story['audio_url'])


def test_parser_audio_jsonfeed():
    result = _parse_well_feed('jsonfeed_audio.json')
    expect = r'http://therecord\.co/downloads/.+\.m4a'
    for story in result.storys:
        assert story['audio_url']
        assert re.match(expect, story['audio_url'])
