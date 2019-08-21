import enum
import json
import gzip
import msgpack

from .helper import shorten


class ActorMessageError(Exception):
    pass


class UnsupportContentEncodingError(ActorMessageError):
    pass


class ActorMessageEncodeError(ActorMessageError):
    pass


class ActorMessageDecodeError(ActorMessageError):
    pass


class ContentEncoding(enum.Enum):
    JSON = 'json'
    MSGPACK = 'msgpack'
    JSON_GZIP = 'json+gzip'
    MSGPACK_GZIP = 'msgpack+gzip'

    @staticmethod
    def of(value):
        if value is None:
            return ContentEncoding.JSON
        if not isinstance(value, ContentEncoding):
            try:
                return ContentEncoding(value)
            except ValueError:
                raise UnsupportContentEncodingError(f'unsupport content encoding {value}')
        return value

    @property
    def is_json(self):
        return self in (ContentEncoding.JSON, ContentEncoding.JSON_GZIP)

    @property
    def is_msgpack(self):
        return self in (ContentEncoding.MSGPACK, ContentEncoding.MSGPACK_GZIP)

    @property
    def is_gzip(self):
        return self in (ContentEncoding.JSON_GZIP, ContentEncoding.MSGPACK_GZIP)


class ActorMessage:
    def __init__(
        self, *,
        id: str = None,
        content: dict = None, is_ask: bool = False,
        src: str = None, src_node: str = None,
        dst: str, dst_node: str = None, dst_url: str = None,
    ):
        self.id = id
        if content is None:
            content = {}
        self.content = content
        self.is_ask = is_ask
        self.src = src
        self.src_node = src_node
        self.dst = dst
        self.dst_node = dst_node
        self.dst_url = dst_url

    def __repr__(self):
        ask = 'ask' if self.is_ask else 'tell'
        return '<{} {} {}/{} {} {}/{} {}>'.format(
            type(self).__name__, self.id,
            self.src_node, self.src, ask, self.dst_node, self.dst,
            shorten(repr(self.content), width=30),
        )

    @classmethod
    def from_dict(cls, d):
        return ActorMessage(
            id=d['id'], content=d['content'], is_ask=d['is_ask'],
            src=d['src'], src_node=d['src_node'],
            dst=d['dst'], dst_node=d['dst_node'], dst_url=d['dst_url'],
        )

    def to_dict(self):
        return dict(
            id=self.id, content=self.content, is_ask=self.is_ask,
            src=self.src, src_node=self.src_node,
            dst=self.dst, dst_node=self.dst_node, dst_url=self.dst_url,
        )

    @classmethod
    def raw_encode(cls, data, content_encoding=None):
        content_encoding = ContentEncoding.of(content_encoding)
        try:
            if content_encoding.is_json:
                data = json.dumps(data, ensure_ascii=False).encode('utf-8')
            else:
                data = msgpack.packb(data, use_bin_type=True)
        except (ValueError, TypeError) as ex:
            raise ActorMessageEncodeError(str(ex)) from ex
        if content_encoding.is_gzip:
            data = gzip.compress(data)
        return data

    @classmethod
    def raw_decode(cls, data, content_encoding=None):
        content_encoding = ContentEncoding.of(content_encoding)
        if content_encoding.is_gzip:
            try:
                data = gzip.decompress(data)
            except (ValueError, TypeError):
                raise ActorMessageDecodeError('gzip decompress failed')
        try:
            if content_encoding.is_json:
                data = json.loads(data.decode('utf-8'))
            else:
                data = msgpack.unpackb(data, raw=False)
        except json.JSONDecodeError:
            raise ActorMessageDecodeError('json decode failed')
        except msgpack.UnpackException:
            raise ActorMessageDecodeError('msgpack decode failed')
        return data

    @classmethod
    def batch_encode(cls, messages, content_encoding=None):
        items = [x.to_dict() for x in messages]
        return cls.raw_encode(items, content_encoding=content_encoding)

    @classmethod
    def batch_decode(cls, data, content_encoding=None):
        items = cls.raw_decode(data, content_encoding=content_encoding)
        try:
            messages = [cls.from_dict(x) for x in items]
        except KeyError as ex:
            raise ActorMessageDecodeError(str(ex)) from ex
        return messages
