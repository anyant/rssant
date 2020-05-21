import binascii
import struct


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


class SeaweedFileType:
    HEADER = 1
    CONTENT = 2


def seaweed_fid_for(feed_id: int, offset: int, ftype: int) -> str:
    """
    返回seaweedfs文件ID，其中cookie值设为0

    file key: 64 bits
    +---------+---------+--------+-------+
    |    28   |    4    |   28   |   4   |
    +---------+---------+--------+-------+
    | feed_id | reserve | offset | ftype |
    +---------+---------+----------------+

    https://github.com/chrislusf/seaweedfs/blob/master/README.md#save-file-id
    """
    reserve = 0
    volume_id = seaweed_volume_for(feed_id)
    file_key = (feed_id << 36) + (reserve << 32) + (offset << 4) + ftype
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
    feed_id = file_key >> 36
    reserve = (file_key >> 32) & 0b00001111
    offset = (file_key >> 4) & 0x0fffffff
    ftype = file_key & 0b00001111
    return (volume_id, feed_id, reserve, offset, ftype)
