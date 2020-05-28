import json
import gzip
import datetime
import struct

from validr import T
from rssant_common.validator import compiler
from .story_sharding import seaweed_fid_encode, SeaweedFileType
from .seaweed_client import SeaweedClient


_dump_datetime = compiler.compile(T.datetime)


def _json_default(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return _dump_datetime(obj)
    raise TypeError("Type %s not serializable" % type(obj))


class SeaweedData:
    def __init__(self, value: bytes, version: int = 1):
        self._value = value
        self._version = version

    @property
    def value(self) -> bytes:
        return self._value

    @property
    def version(self) -> int:
        return self.version

    def encode(self) -> bytes:
        version = struct.pack('>B', self._version)
        data_bytes = gzip.compress(self._value)
        return version + data_bytes

    @classmethod
    def decode(cls, data: bytes) -> "SeaweedData":
        (version,) = struct.unpack('>B', data[:1])
        if version != 1:
            raise ValueError(f'not support version {version}')
        value = gzip.decompress(data[1:])
        return cls(value, version=version)

    @classmethod
    def encode_json(cls, value: dict, version: int = 1) -> bytes:
        value = json.dumps(value, ensure_ascii=False, default=_json_default).encode('utf-8')
        return cls(value, version=version).encode()

    @classmethod
    def decode_json(cls, data: bytes) -> dict:
        value = cls.decode(data).value
        return json.loads(value.decode('utf-8'))

    @classmethod
    def encode_text(cls, value: str, version: int = 1) -> bytes:
        value = value.encode('utf-8')
        return cls(value, version=version).encode()

    @classmethod
    def decode_text(cls, data: bytes) -> str:
        value = cls.decode(data).value
        return value.decode('utf-8')


class SeaweedStoryStorage:
    def __init__(self, client: SeaweedClient):
        self._client: SeaweedClient = client

    def _content_fid(self, feed_id: int, offset: int) -> str:
        return seaweed_fid_encode(feed_id, offset, SeaweedFileType.CONTENT)

    def get_content(self, feed_id: int, offset: int) -> str:
        content_data = self._client.get(self._content_fid(feed_id, offset))
        if content_data:
            content = SeaweedData.decode_text(content_data)
        else:
            content = None
        return content

    def delete_content(self, feed_id: int, offset: int) -> None:
        self._client.delete(self._content_fid(feed_id, offset))

    def save_content(self, feed_id: int, offset: int, content: str) -> None:
        if not content:
            return self.delete_content(feed_id, offset)
        content_data = SeaweedData.encode_text(content)
        self._client.put(self._content_fid(feed_id, offset), content_data)
