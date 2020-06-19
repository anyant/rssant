import gzip
import struct


class StoryUniqueIdsData:
    """
    +---------+--------------+-----------------+
    |  1 byte |    4 bytes   |   about 10KB    |
    +---------+--------------+-----------------+
    | version | begin_offset | unique_ids_gzip |
    +---------+--------------+-----------------+
    """

    def __init__(self, begin_offset: int, unique_ids: list, version=1):
        self._version = version
        self._begin_offset = begin_offset
        for x in unique_ids:
            if not x:
                raise ValueError('unique_id can not be empty')
            if '\n' in x:
                raise ValueError(r"unique_id can not contains '\n' character")
        self._unique_ids = unique_ids

    @property
    def unique_ids(self) -> list:
        return self._unique_ids

    @property
    def begin_offset(self) -> int:
        return self._begin_offset

    def encode(self) -> bytes:
        value = '\n'.join(self._unique_ids).encode('utf-8')
        unique_ids_gzip = gzip.compress(value)
        header = struct.pack('>BI', self._version, self._begin_offset)
        return header + unique_ids_gzip

    @classmethod
    def decode(cls, data: bytes) -> "StoryUniqueIdsData":
        (version,) = struct.unpack('>B', data[:1])
        if version != 1:
            raise ValueError(f'not support version {version}')
        (begin_offset,) = struct.unpack('>I', data[1:5])
        value = gzip.decompress(data[5:]).decode('utf-8')
        unique_ids = value.split('\n') if value else []
        return cls(begin_offset, unique_ids, version=version)
