import datetime
import pytest
from rssant_api.models.story_storage import StoryData


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


SAMPLE_TEXT = 'hello world\n你好世界\n'


@pytest.mark.parametrize('length', [0, 255, 1024, 16 * 1024, 64 * 1024])
def test_encode_decode_text(length):
    text = (SAMPLE_TEXT * (length // len(SAMPLE_TEXT)))[:length]
    data = StoryData.encode_text(text)
    got = StoryData.decode_text(data)
    assert got == text
