import typing
import struct
import binascii


def hash_feed_id(feed_id: int) -> int:
    data = struct.pack('>I', feed_id)
    value = binascii.crc32(data) & 0xffffffff
    return value


class StoryKey:
    """
    story key: 64 bits
    +----------+---------+--------+----------+
    |     4    |   28    |   28   |    4     |
    +----------+---------+--------+----------+
    | reserve1 | feed_id | offset | reserve2 |
    +----------+---------+-------------------+
    """

    @staticmethod
    def encode(feed_id: int, offset: int, reserve1: int = 0, reserve2: int = 0) -> int:
        assert 0 <= reserve1 <= 255, 'expect 0 <= reserve1 <= 255'
        assert 0 <= reserve2 <= 255, 'expect 0 <= reserve2 <= 255'
        assert 0 <= feed_id <= 0x0fffffff, 'expect 0 <= feed_id <= 0x0fffffff'
        assert 0 <= offset <= 0x0fffffff, 'expect 0 <= offset <= 0x0fffffff'
        return (reserve1 << 60) + (feed_id << 32) + (offset << 4) + reserve2

    @staticmethod
    def decode(key: int) -> typing.Tuple[int, int, int, int]:
        reserve2 = key & 0b00001111
        offset = (key >> 4) & 0x0fffffff
        feed_id = (key >> 32) & 0x0fffffff
        reserve1 = (key >> 60) & 0b00001111
        return feed_id, offset, reserve1, reserve2


class StoryId:
    """
    virtual story id, composited by feed_id and offset
    """

    @staticmethod
    def encode(feed_id: int, offset: int) -> int:
        return StoryKey.encode(feed_id, offset)

    @staticmethod
    def decode(story_id: int) -> typing.Tuple[int, int]:
        feed_id, offset, __, __ = StoryKey.decode(story_id)
        return feed_id, offset
