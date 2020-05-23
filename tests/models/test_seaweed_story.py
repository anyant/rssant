import datetime

import pytest

from rssant_api.models.seaweed_story import (
    SeaweedData, SeaweedStory, SeaweedStoryStorage)


class MockSeaweedClient:
    def __init__(self):
        self._store = {}

    def get(self, fid: str) -> bytes:
        return self._store.get(fid)

    def put(self, fid: str, data: bytes) -> None:
        self._store[fid] = data

    def delete(self, fid: str) -> None:
        self._store.pop(fid, None)


def test_encode_decode_json():
    dt = datetime.datetime(2020, 5, 23, 12, 12, 12, tzinfo=datetime.timezone.utc)
    base = {
        'key': 'value',
        'text': '你好',
        'number': 123,
    }
    value = {**base, 'datetime': dt}
    expect = {**base, 'datetime': '2020-05-23T12:12:12.000000Z'}
    data = SeaweedData.encode_json(value)
    got = SeaweedData.decode_json(data)
    assert got == expect


def test_encode_decode_text():
    text = 'hello world\n你好世界\n'
    data = SeaweedData.encode_text(text)
    got = SeaweedData.decode_text(data)
    assert got == text


CONTENTS = {
    'empty': None,
    'simple': 'hello world\n你好世界\n',
}


@pytest.mark.parametrize('content_name', list(CONTENTS))
def test_seaweed_story_storage(content_name):
    client = MockSeaweedClient()
    storage = SeaweedStoryStorage(client)
    dt = datetime.datetime(2020, 5, 23, 12, 12, 12, tzinfo=datetime.timezone.utc)
    content = CONTENTS[content_name]
    story = SeaweedStory(
        feed_id=123,
        offset=234,
        unique_id='https://www.example.com/1.html',
        title='hello world',
        link='https://www.example.com/1.html',
        author='',
        image_url='https://www.example.com/1.png',
        dt_published=dt,
        dt_updated=dt,
        dt_created=dt,
        summary='hello world hello world',
        content=content,
        content_length=len(content or ''),
        content_hash_base64=None,
    )
    storage.save_story(story)
    got = storage.get_story(123, 234, include_content=True)
    assert got == story
    storage.delete_story(123, 234)
    got = storage.get_story(123, 234, include_content=True)
    assert got is None
