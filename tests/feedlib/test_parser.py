import logging
from pathlib import Path

import pytest

from rssant_feedlib import (
    RawFeedParser, FeedParser, FeedParserError, FeedResponseBuilder,
)


LOG = logging.getLogger(__name__)


_data_dir = Path(__file__).parent / 'testdata/parser'


def _collect_filenames(base_dir):
    names = [x.name for x in Path(base_dir).glob('*')]
    return names


def _read_response(base_dir, filename):
    content = (Path(base_dir) / filename).read_bytes()
    builder = FeedResponseBuilder()
    builder.url('https://blog.example.com/feed')
    builder.content(content)
    response = builder.build()
    return response


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
