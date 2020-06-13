import datetime

import pytest

from rssant_api.models.story_storage import SeaweedStoryStorage
from rssant_api.models.story_storage import StoryData


class MockSeaweedClient:
    def __init__(self):
        self._store = {}

    def get(self, fid: str) -> bytes:
        return self._store.get(fid)

    def batch_get(self, fid_s: list) -> list:
        return {fid: self._store.get(fid) for fid in fid_s}

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
    data = StoryData.encode_json(value)
    got = StoryData.decode_json(data)
    assert got == expect


def test_encode_decode_text():
    text = 'hello world\n你好世界\n'
    data = StoryData.encode_text(text)
    got = StoryData.decode_text(data)
    assert got == text


CONTENTS = {
    'empty': None,
    'simple': 'hello world\n你好世界\n',
}


@pytest.mark.parametrize('content_name', list(CONTENTS))
def test_seaweed_story_storage(content_name):
    client = MockSeaweedClient()
    storage = SeaweedStoryStorage(client)
    content = CONTENTS[content_name]
    storage.save_content(123, 234, content)
    got = storage.get_content(123, 234)
    assert got == content
    storage.delete_content(123, 234)
    got = storage.get_content(123, 234)
    assert got is None
