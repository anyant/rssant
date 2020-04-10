from typing import List, Tuple
from collections import OrderedDict
import itertools
import struct
import hashlib
import brotli


class FeedChecksum:
    """
    At most: 8 * 2 * 500 = 8KB, about 3KB after brotli compress
    +---------+------------------+--------------------+
    | 1 byte  |             8 * 2 * N bytes           |
    +---------+------------------+--------------------+
    | version | story_ident_hash | story_content_hash |
    +---------+------------------+--------------------+
    """

    def __init__(self, items: List[Tuple[bytes, bytes]] = None, version: int = 1):
        self.version = version
        self._map = OrderedDict()
        for key, value in items or []:
            self._check_key_value(key, value)
            self._map[key] = value

    def __repr__(self):
        return '<{} version={} items={}>'.format(
            type(self).__name__, self.version, len(self._map))

    def copy(self) -> "FeedChecksum":
        items = list(self._map.items())
        return FeedChecksum(items, version=self.version)

    def _hash(self, value: str) -> bytes:
        """8 bytes md5"""
        return hashlib.md5(value.encode('utf-8')).digest()[:8]

    def update(self, ident: str, content: str) -> bool:
        """
        由于哈希碰撞，可能会出现:
            1. 有更新但内容哈希值没变，导致误判为无更新
            2. 多个ID哈希值一样，导致误判为有更新
        """
        key = self._hash(ident)
        old_sum = self._map.get(key)
        new_sum = self._hash(content)
        if (not old_sum) or old_sum != new_sum:
            self._map[key] = new_sum
            return True
        return False

    def _check_key_value(self, key: bytes, value: bytes):
        assert len(key) == 8, 'key length must be 8 bytes'
        assert len(value) == 8, 'value length must be 8 bytes'

    def dump(self, limit=None) -> bytes:
        length = len(self._map)
        buffer_n = length if limit is None else min(length, limit)
        buffer = bytearray(1 + 16 * buffer_n)
        struct.pack_into('>B', buffer, 0, self.version)
        offset = 1
        items = self._map.items()
        if limit is not None and length > limit:
            items = itertools.islice(items, length - limit)
        for key, value in items:
            self._check_key_value(key, value)
            buffer[offset: offset + 8] = key
            buffer[offset + 8: offset + 16] = value
            offset += 16
        buffer = brotli.compress(buffer)
        return buffer

    @classmethod
    def load(cls, data: bytes) -> "FeedChecksum":
        data = brotli.decompress(data)
        version = struct.unpack('>B', data[:1])
        if version != 1:
            raise ValueError(f'not support version {version}')
        n, remain = divmod(len(data) - 1, 16)
        if remain != 0:
            raise ValueError(f'unexpect data length {len(data)}')
        items = []
        for i in range(n):
            offset = 1 + i * 8
            key = data[offset: offset + 8]
            value = data[offset + 8: offset + 16]
            items.append((key, value))
        return cls(items, version=version)
