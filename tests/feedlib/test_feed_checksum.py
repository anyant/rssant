import random
import time

import pytest

from rssant_feedlib.feed_checksum import FeedChecksum


def _random_unicode(length: int) -> str:

    # Update this to include code point ranges to be sampled
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C),
    ]

    alphabet = [
        chr(code_point) for current_range in include_ranges
        for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(random.choice(alphabet) for i in range(length))


def _random_storys(n):
    storys = []
    for i in range(n):
        ident = str(i) * 20
        content = _random_unicode(random.randint(1, 1000))
        storys.append((ident, content))
    return storys


def test_feed_checksum_basic():
    storys = _random_storys(100)
    checksum = FeedChecksum()
    for ident, content in storys:
        assert checksum.update(ident, content) is True
    assert checksum.size() == 100
    assert repr(checksum)
    checksum_bak = checksum.copy()
    assert checksum_bak == checksum
    for ident, content in storys:
        assert checksum.update(ident, content) is False
    assert checksum_bak == checksum


def test_feed_checksum_dump_load():
    storys = _random_storys(1000)
    checksum = FeedChecksum()
    for ident, content in storys:
        assert checksum.update(ident, content) is True
    assert checksum.size() == 1000
    assert repr(checksum)
    for ident, content in storys:
        assert checksum.update(ident, content) is False

    data = checksum.dump()
    loaded = FeedChecksum.load(data)
    assert loaded.size() == 1000
    assert loaded == checksum

    data = checksum.dump(limit=500)
    loaded = FeedChecksum.load(data)
    assert loaded.size() == 500
    assert loaded != checksum

    # verify only dump last N items
    checksum = loaded
    for ident, content in storys[500:]:
        assert checksum.update(ident, content) is False
    assert checksum.size() == 500
    for ident, content in storys[:500]:
        assert checksum.update(ident, content) is True
    assert checksum.size() == 1000


def test_feed_checksum_error():
    # format: 4 bytes + 8 bytes
    items = [(b'1234', b'12345678')]
    checksum = FeedChecksum(items)
    assert checksum.size() == 1

    data = checksum.dump()
    data = data + b'123'
    with pytest.raises(ValueError):
        FeedChecksum.load(data)

    items = [(b'123', b'123456')]
    with pytest.raises(ValueError):
        FeedChecksum(items)

    items = [(b'12', b'12345')]
    with pytest.raises(ValueError):
        FeedChecksum(items)


def _format_t(t):
    return '{:.1f}ms'.format(t * 1000)


def test_benchmark_feed_checksum():
    for n in range(100, 1001, 100):
        storys = _random_storys(n)
        checksum = FeedChecksum()
        t0 = time.monotonic()
        for ident, content in storys:
            if not checksum.update(ident, content):
                print(f'n={n} content conflict')
        t1 = time.monotonic()
        for ident, content in storys:
            if checksum.update(ident, content):
                print(f'n={n} ident conflict')
        t2 = time.monotonic()
        data = checksum.dump()
        t3 = time.monotonic()
        print(f'{n} items, {len(data)} bytes')
        FeedChecksum.load(data)
        t4 = time.monotonic()
        print('{} items, update: {} + {}'.format(
            n, _format_t(t1 - t0), _format_t(t2 - t1)))
        print('{} items, dump {}, load {}'.format(
            n, _format_t(t3 - t2), _format_t(t4 - t3)))


if __name__ == "__main__":
    test_benchmark_feed_checksum()
