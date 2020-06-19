

VOLUME_SIZE = 64 * 1024


def sharding_for(feed_id: int) -> int:
    """
    数据分片算法，按 FeedID 范围分片。
    每卷存储 64K 订阅的故事数据，大约64GB，1千万行记录。
    """
    return feed_id // VOLUME_SIZE
