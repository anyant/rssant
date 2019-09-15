import enum
import json
import gzip
import time
import msgpack
from concurrent.futures import Future

from .helper import shorten, format_timestamp


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
        content: dict = None,
        priority: int = None,
        is_ask: bool = False,
        require_ack: bool = False,
        src: str = None,
        src_node: str = None,
        dst: str,
        dst_node: str = None,
        is_local: bool = None,
        expire_at: int = None,
        parent_id: str = None,
        future: Future = None,
    ):
        self.id = id
        self.content = content
        if is_ask:
            if priority is not None and priority != 0:
                raise ValueError('ask message can not set priority')
            priority = 0
        else:
            if priority is None:
                priority = 100
            priority = max(1, priority)
        self.priority = priority
        if is_ask and require_ack:
            raise ValueError('ask message not require ack')
        self.is_ask = is_ask
        self.require_ack = require_ack
        self.src = src
        self.src_node = src_node
        self.dst = dst
        self.dst_node = dst_node
        self.is_local = is_local
        if expire_at is not None:
            if expire_at <= 0:
                expire_at = None
            else:
                expire_at = int(expire_at)
        if is_ask and expire_at:
            raise ValueError('ask message can not set expire_at')
        self.expire_at = expire_at
        self.parent_id = parent_id
        self.future = future

    def __eq__(self, other: "ActorMessage"):
        return all([
            self.id == other.id,
            self.dst == other.dst,
        ])

    def __lt__(self, other: "ActorMessage"):
        return (self.priority, id(self)) < (other.priority, id(other))

    def __repr__(self):
        type_name = type(self).__name__
        if self.is_ask:
            msg_type = '?'
        else:
            msg_type = '!' if self.require_ack else '~'
        expire_at = ''
        if self.expire_at is not None:
            expire_at = ' expire_at=' + format_timestamp(self.expire_at)
        short_content = shorten(repr(self.content), width=30)
        parent = ''
        if self.parent_id:
            parent = ' parent=' + self.parent_id
        return (f'<{type_name} {self.id} {self.src_node}/{self.src} {msg_type} '
                f'{self.dst_node}/{self.dst}{expire_at}{parent} {short_content}>')

    def is_expired(self, now: int = None):
        if self.expire_at is None:
            return False
        if now is None:
            now = time.time()
        return self.expire_at <= now

    @classmethod
    def from_dict(cls, d):
        return ActorMessage(
            id=d['id'],
            priority=d['priority'],
            require_ack=d['require_ack'],
            src=d['src'],
            src_node=d['src_node'],
            dst=d['dst'],
            dst_node=d['dst_node'],
            expire_at=d.get('expire_at'),
            content=d.get('content'),
            is_ask=d.get('is_ask'),
            is_local=d.get('is_local'),
            parent_id=d.get('parent_id'),
        )

    def _to_dict_basic(self):
        return dict(
            id=self.id,
            priority=self.priority,
            require_ack=self.require_ack,
            src=self.src,
            src_node=self.src_node,
            dst=self.dst,
            dst_node=self.dst_node,
            expire_at=self.expire_at,
        )

    def to_dict(self):
        d = self._to_dict_basic()
        d.update(content=self.content)
        return d

    def meta(self):
        d = self._to_dict_basic()
        d.update(
            is_ask=self.is_ask,
            is_local=self.is_local,
            parent_id=self.parent_id,
        )
        return self.from_dict(d)

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
