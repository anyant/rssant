import json
import gzip
import datetime
import struct
import typing

from validr import T, modelclass, asdict
from rssant_common.validator import compiler
from .story_sharding import seaweed_fid_for, SeaweedFileType
from .seaweed_client import SeaweedClient


@modelclass(compiler=compiler)
class SeaweedStory:
    feed_id: int = T.int
    offset: int = T.int
    unique_id: str = T.str
    title: str = T.str
    link: str = T.str.optional
    author: str = T.str.optional
    image_url: str = T.str.optional
    audio_url: str = T.str.optional
    iframe_url: str = T.str.optional
    has_mathjax: bool = T.bool.optional
    dt_published: datetime.datetime = T.datetime.object.optional
    dt_updated: datetime.datetime = T.datetime.object.optional
    dt_created: datetime.datetime = T.datetime.object.optional
    dt_synced: datetime.datetime = T.datetime.object.optional
    summary: str = T.str.optional
    content: str = T.str.optional
    content_length: int = T.int.min(0).optional
    content_hash_base64: str = T.str.optional


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
        self._client = client

    def _get_by_fid(self, fid: str) -> bytes:
        return self._client.get(fid)

    def _put_by_fid(self, fid: str, data: bytes):
        return self._client.put(fid, data)

    def _delete_by_fid(self, fid: str):
        self._client.delete(fid)

    def _get_content_by_offset(self, feed_id, offset) -> bytes:
        fid = seaweed_fid_for(feed_id, offset, SeaweedFileType.CONTENT)
        return self._get_by_fid(fid)

    def get_story(self, feed_id, offset, include_content=False) -> SeaweedStory:
        storys = self.batch_get_story([(feed_id, offset)], include_content=include_content)
        return storys[0] if storys else None

    def _batch_get_by_keys(self, story_keys, ftype):
        fid_s = {}
        for feed_id, offset in story_keys:
            fid = seaweed_fid_for(feed_id, offset, ftype)
            fid_s[fid] = (feed_id, offset)
        data_s = self._client.batch_get(fid_s.keys())
        for fid, data in data_s.items():
            if data:
                yield fid_s[fid], data

    def batch_get_story(self, story_keys, include_content=False) -> typing.List[SeaweedStory]:
        header_data_s = list(self._batch_get_by_keys(story_keys, SeaweedFileType.HEADER))
        story_s = []
        for (feed_id, offset), header_data in header_data_s:
            story = SeaweedData.decode_json(header_data)
            story_s.append(((feed_id, offset), story))
        if include_content:
            content_keys = []
            for (feed_id, offset), story in story_s:
                content_length = story.get('content_length', 0)
                has_content = (content_length is not None) and content_length > 0
                if has_content:
                    content_keys.append((feed_id, offset))
            content_data_map = dict(self._batch_get_by_keys(content_keys, SeaweedFileType.CONTENT))
            for (feed_id, offset), story in story_s:
                content_data = content_data_map.get((feed_id, offset))
                if content_data:
                    content = SeaweedData.decode_text(content_data)
                else:
                    content = ''
                story['content'] = content
        result = []
        for _, story in story_s:
            result.append(SeaweedStory(story))
        return result

    def save_story(self, story: SeaweedStory):
        header = asdict(story)
        content = header.pop('content', None) or ''
        if content:
            header.update(content_length=len(content))
        else:
            header.setdefault('content_length', len(content))
        header_data = SeaweedData.encode_json(header)
        fid = seaweed_fid_for(story.feed_id, story.offset, SeaweedFileType.HEADER)
        self._put_by_fid(fid, header_data)
        if content:
            fid_c = seaweed_fid_for(story.feed_id, story.offset, SeaweedFileType.CONTENT)
            content_data = SeaweedData.encode_text(content)
            self._put_by_fid(fid_c, content_data)

    def delete_story(self, feed_id: int, offset: int):
        fid = seaweed_fid_for(feed_id, offset, SeaweedFileType.HEADER)
        fid_c = seaweed_fid_for(feed_id, offset, SeaweedFileType.CONTENT)
        self._delete_by_fid(fid)
        self._delete_by_fid(fid_c)
