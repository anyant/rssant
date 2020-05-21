import random
from collections import defaultdict

from rssant_api.models.story_sharding import (
    hash_feed_id, sharding_for, seaweed_volume_for,
    SeaweedFileType, seaweed_fid_for, seaweed_fid_decode,
)


def test_hash_feed_id():
    for i in [0, 1, 2, 7, 1024, 2**31, 2**32 - 1]:
        val = hash_feed_id(i)
        assert val >= 0 and val < 2**32


def test_sharding_for_0():
    for i in [0, 1, 2, 1, 8 * 1024 - 1]:
        assert sharding_for(i) == 0
    assert sharding_for(8 * 1024) > 0
    assert sharding_for(8 * 1024 + 1) > 0


def test_seaweed_volume_for():
    assert seaweed_volume_for(0) == 1
    assert seaweed_volume_for(8 * 1024 - 1) == 1
    assert seaweed_volume_for(8 * 1024) > 1


def test_sharding_for_group():
    for i in [8 * 1024, 8 * 1024 + 1, 64 * 1024, (8 + 64) * 1024 - 1]:
        val = sharding_for(i)
        assert val >= 1 and val < 9
    for i in [(8 + 64) * 1024, (64 * 2) * 1024, (8 + 64 * 2) * 1024 - 1]:
        val = sharding_for(i)
        assert val >= 9 and val < 17


def test_sharding_for_uniform():
    volumes = defaultdict(lambda: 0)
    N = 100000
    for i in range(N):
        feed_id = random.randint(0, (8 + 64) * 1024 - 1)
        volumes[sharding_for(feed_id)] += 1
    min_count = min(volumes.values())
    max_count = max(volumes.values())
    msg = f'min={min_count} max={max_count} volumes={dict(volumes)}'
    assert (max_count - min_count) / N < 0.01, msg


def test_seaweed_fid():
    cases = [
        (123, 10, SeaweedFileType.HEADER, '1,7b0000000a100000000'),
        (123, 1023, SeaweedFileType.HEADER, '1,7b000003ff100000000'),
        (123, 1023, SeaweedFileType.CONTENT, '1,7b000003ff200000000'),
    ]
    for feed_id, offset, ftype, expect in cases:
        fid = seaweed_fid_for(feed_id, offset, ftype)
        msg = f'expect {feed_id, offset, ftype} -> {expect}, got {fid}'
        assert fid == expect, msg
        _, x_feed_id, _, x_offset, x_ftype = seaweed_fid_decode(fid)
        assert x_feed_id == feed_id
        assert x_offset == offset
        assert x_ftype == ftype
