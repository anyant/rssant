import json
import gzip
import datetime
import struct

from validr import T
import lz4.frame as lz4

from rssant_common.validator import compiler


_dump_datetime = compiler.compile(T.datetime)


def _json_default(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return _dump_datetime(obj)
    raise TypeError("Type %s not serializable" % type(obj))


class StoryData:

    VERSION_GZIP = 1
    VERSION_LZ4 = 2

    __slots__ = ('_value', '_version')

    def __init__(self, value: bytes, version: int):
        self._value = value
        self._check_version(version)
        self._version = version

    @classmethod
    def _check_version(cls, version: int):
        supported = (cls.VERSION_GZIP, cls.VERSION_LZ4,)
        if version not in supported:
            raise ValueError(f'not support version {version}')

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def version(self) -> int:
        return self._version

    def encode(self) -> bytes:
        version = struct.pack('>B', self._version)
        if self._version == self.VERSION_GZIP:
            data_bytes = gzip.compress(self._value)
        elif self._version == self.VERSION_LZ4:
            data_bytes = lz4.compress(self._value)
        else:
            assert False, f'unknown version {version}'
        return version + data_bytes

    @classmethod
    def decode(cls, data: bytes) -> "StoryData":
        (version,) = struct.unpack('>B', data[:1])
        cls._check_version(version)
        if version == cls.VERSION_GZIP:
            value = gzip.decompress(data[1:])
        elif version == cls.VERSION_LZ4:
            value = lz4.decompress(data[1:])
        else:
            assert False, f'unknown version {version}'
        return cls(value, version=version)

    @classmethod
    def encode_json(cls, value: dict, version: int = VERSION_LZ4) -> bytes:
        value = json.dumps(value, ensure_ascii=False, default=_json_default).encode('utf-8')
        return cls(value, version=version).encode()

    @classmethod
    def decode_json(cls, data: bytes) -> dict:
        value = cls.decode(data).value
        return json.loads(value.decode('utf-8'))

    @classmethod
    def encode_text(cls, value: str, version: int = VERSION_LZ4) -> bytes:
        value = value.encode('utf-8')
        return cls(value, version=version).encode()

    @classmethod
    def decode_text(cls, data: bytes) -> str:
        value = cls.decode(data).value
        return value.decode('utf-8')
