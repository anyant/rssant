import logging
import time
import pytest
from rssant_common.image_token import ImageToken, ImageTokenDecodeError, ImageTokenExpiredError


LOG = logging.getLogger(__name__)

referrer = 'https://www.example.com/page/1.html'


def test_encode_decode():
    token = ImageToken(referrer=referrer)
    assert repr(token)
    got = ImageToken.decode(token.encode(secret='test'), secret='test')
    assert got.referrer == referrer
    assert got.feed is None
    assert got.offset is None
    assert got.timestamp == token.timestamp

    token = ImageToken(referrer=referrer, feed=123, offset=0)
    assert repr(token)
    got = ImageToken.decode(token.encode(secret='test'), secret='test')
    assert got.referrer == referrer
    assert got.feed == 123
    assert got.offset == 0
    assert got.timestamp == token.timestamp


def test_decode_error():
    with pytest.raises(ImageTokenDecodeError):
        ImageToken.decode('aaa', secret='test')
    with pytest.raises(ImageTokenDecodeError):
        ImageToken.decode('aaa.bbb', secret='test')


def test_decode_signature():
    token = ImageToken().encode(secret='test')
    with pytest.raises(ImageTokenDecodeError):
        ImageToken.decode(token, secret='xxx')


def test_decode_expires():
    timestamp = 1600000000
    token = ImageToken(timestamp=timestamp).encode(secret='test')

    def clock():
        return 1600001000

    got = ImageToken.decode(token, secret='test', expires=1001, clock=clock)
    assert got.timestamp == timestamp
    with pytest.raises(ImageTokenExpiredError):
        ImageToken.decode(token, secret='test', expires=999, clock=clock)


def test_performance():
    token = ImageToken(referrer=referrer, feed=123, offset=0)
    t0 = time.time()
    for i in range(1000):
        ImageToken.decode(token.encode(secret='test'), secret='test')
    cost = int((time.time() - t0) * 1000)
    LOG.info('ImageToken 1000 encode + decode cost {}ms'.format(cost))
