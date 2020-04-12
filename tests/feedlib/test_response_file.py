import pytest

from rssant_feedlib import FeedResponseBuilder
from rssant_feedlib.response_file import FeedResponseFile


def _build_simple_response(status):
    builder = FeedResponseBuilder()
    builder.url('https://www.example.com/feed.xml')
    builder.status(status)
    builder.headers({
        'etag': '5e8c43f8-4d269b',
        'content-type': 'application/xml',
    })
    builder.content('''
        <?xml version="1.0" encoding="utf-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
        <title>V2EX - 技术</title>
    '''.encode('utf-8'))
    response = builder.build()
    return response


def _build_no_content_response(status):
    builder = FeedResponseBuilder()
    builder.url('https://www.example.com/feed.xml')
    builder.status(status)
    response = builder.build()
    return response


_builder_funcs = {
    'simple': _build_simple_response,
    'no_content': _build_no_content_response,
}


@pytest.mark.parametrize('builder, status', [
    ('simple', 200),
    ('simple', 500),
    ('no_content', 204),
    ('no_content', -200),
])
def test_response_file(tmp_path, builder, status):
    response = _builder_funcs[builder](status)
    file = FeedResponseFile(tmp_path / 'test_response_file')
    file.write(response)
    file2 = FeedResponseFile(file.filepath)
    response2 = file2.read()
    assert repr(response) == repr(response2)
