import binascii
import struct
import typing


VOLUME_SIZE = 8 * 1024
VOLUME_GROUP = 8
VOLUME_GROUP_SIZE = VOLUME_GROUP * VOLUME_SIZE


def hash_feed_id(feed_id: int) -> int:
    data = struct.pack('>I', feed_id)
    value = binascii.crc32(data) & 0xffffffff
    return value


def sharding_for(feed_id: int) -> int:
    """
    数据分片算法，按 FeedID 先范围分组，组内再哈希分片。
    每卷存储 8K 订阅的故事数据，每 8 卷为一组。
    前 8K 订阅固定分在首卷，用于兼顾小规模部署无需分片的场景。

    [0       , 8K          ) -> 0
    [8K      , 8K + 64K    ) -> [1, 9)
    [8K + 64K, 8K + 64K * 2) -> [9, 17)
    """
    if feed_id < VOLUME_SIZE:
        return 0
    group = (feed_id - VOLUME_SIZE) // VOLUME_GROUP_SIZE
    index = hash_feed_id(feed_id) % VOLUME_GROUP
    return group * VOLUME_GROUP + index + 1


def seaweed_volume_for(feed_id: int) -> int:
    """
    返回seaweedfs数据卷ID
    """
    return sharding_for(feed_id) + 1


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


class SeaweedFileType:
    CONTENT = 2


def seaweed_fid_encode(feed_id: int, offset: int, ftype: int) -> str:
    """
    返回seaweedfs文件ID，其中cookie值设为0
    https://github.com/chrislusf/seaweedfs/blob/master/README.md#save-file-id
    """
    volume_id = seaweed_volume_for(feed_id)
    file_key = StoryKey.encode(feed_id, offset, reserve2=ftype)
    file_key_hex = '%x' % file_key
    cookie_hex = '00000000'
    return f'{volume_id},{file_key_hex}{cookie_hex}'


def seaweed_fid_decode(fid: str) -> tuple:
    """
    decode seaweed fid, ignore cookie:
        -> (volume_id, feed_id, reserve, offset, ftype)
    """
    try:
        volume_id_str, remain = fid.split(',')
        volume_id = int(volume_id_str)
        file_key = int(remain[:-8], base=16)
    except ValueError as ex:
        raise ValueError('invalid seaweed fid') from ex
    feed_id, offset, __, ftype = StoryKey.decode(file_key)
    return volume_id, feed_id, offset, ftype
