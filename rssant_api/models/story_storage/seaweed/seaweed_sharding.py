from ..common.story_key import StoryKey, hash_feed_id


VOLUME_SIZE = 1 * 1024
VOLUME_GROUP = 8
VOLUME_GROUP_SIZE = VOLUME_GROUP * VOLUME_SIZE


def sharding_for(feed_id: int) -> int:
    """
    数据分片算法，按 FeedID 先范围分组，组内再哈希分片。
    每卷存储 1K 订阅的故事数据(大约1GB)，每 8 卷为一组。

    [  0      ,  8K ) -> [  0,  8 )
    [  8K     , 16K ) -> [  8, 16 )
    [ 16K     , 24K ) -> [ 16, 24 )
    """
    group = feed_id // VOLUME_GROUP_SIZE
    index = hash_feed_id(feed_id) % VOLUME_GROUP
    return group * VOLUME_GROUP + index


def seaweed_volume_for(feed_id: int) -> int:
    """
    返回seaweedfs数据卷ID
    """
    return sharding_for(feed_id) + 1


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
