import enum
import json
import gzip
import time
import datetime
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
        content: dict = None, is_ask: bool = False, require_ack: bool = False,
        src: str = None, src_node: str = None,
        dst: str, dst_node: str = None, dst_url: str = None,
        expire_at: int = None,
    ):
        self.id = id
        if content is None:
            content = {}
        self.content = content
        if is_ask and require_ack:
            raise ValueError('ask message not require ack')
        self.is_ask = is_ask
        self.require_ack = require_ack
        self.src = src
        self.src_node = src_node
        self.dst = dst
        self.dst_node = dst_node
        self.dst_url = dst_url
        if expire_at is not None:
            if expire_at <= 0:
                expire_at = None
            else:
                expire_at = int(expire_at)
        if is_ask and expire_at:
            raise ValueError('ask message can not set expire_at')
        self.expire_at = expire_at

    def __repr__(self):
        type_name = type(self).__name__
        if self.is_ask:
            msg_type = '?'
        else:
            msg_type = '!' if self.require_ack else '~'
        expire_at = ''
        if self.expire_at is not None:
            expire_at = datetime.datetime.utcfromtimestamp(self.expire_at)
            expire_at = ' expire_at ' + expire_at.isoformat(timespec='seconds') + 'Z'
        short_content = shorten(repr(self.content), width=30)
        return (f'<{type_name} {self.id} {self.src_node}/{self.src} {msg_type} '
                f'{self.dst_node}/{self.dst}{expire_at} {short_content}>')

    def is_expired(self, now: int = None):
        if self.expire_at is None:
            return False
        if now is None:
            now = time.time()
        return self.expire_at <= now

    @classmethod
    def from_dict(cls, d):
        return ActorMessage(
            id=d['id'], content=d['content'],
            is_ask=d['is_ask'], require_ack=d['require_ack'],
            src=d['src'], src_node=d['src_node'],
            dst=d['dst'], dst_node=d['dst_node'], dst_url=d['dst_url'],
            expire_at=d.get('expire_at'),
        )

    def to_dict(self):
        return dict(
            id=self.id, content=self.content,
            is_ask=self.is_ask, require_ack=self.require_ack,
            src=self.src, src_node=self.src_node,
            dst=self.dst, dst_node=self.dst_node, dst_url=self.dst_url,
            expire_at=self.expire_at,
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
