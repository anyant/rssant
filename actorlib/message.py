import enum
import json
import gzip
import base64
import msgpack


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
            return ContentEncoding(value)
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
        content: bytes,
        src: str, src_node: str = None,
        dst: str, dst_node: str = None,
        dst_url: str = None,
    ):
        self.src = src
        self.src_node = src_node
        self.dst = dst
        self.dst_node = dst_node
        self.dst_url = dst_url
        self.content = content

    def __repr__(self):
        return '<{} {}/{} to {}/{} size={}>'.format(
            type(self).__name__,
            self.src_node, self.src, self.dst_node, self.dst, len(self.content)
        )

    @classmethod
    def _from_dict(cls, d, do_base64=False):
        content = d['content']
        if do_base64:
            content = base64.b64decode(content.encode('ascii'))
        return ActorMessage(
            src=d['src'], src_node=d['src_node'],
            dst=d['dst'], dst_node=d['dst_node'],
            content=content, dst_url=d['dst_url'],
        )

    @staticmethod
    def _to_dict(self, do_base64=False):
        content = self.content
        if do_base64:
            content = base64.b64encode(content)
        return dict(
            src=self.src, src_node=self.src_node,
            dst=self.dst, dst_node=self.dst_node,
            content=content, dst_url=self.dst_url,
        )

    @classmethod
    def batch_encode(cls, messages, content_encoding=None):
        content_encoding = ContentEncoding.of(content_encoding)
        items = []
        if content_encoding.is_json:
            for x in messages:
                items.append(x._to_dict(do_base64=True))
            data = json.dumps(items, ensure_ascii=False).encode('utf-8')
        else:
            for x in messages:
                items.append(x._to_dict(do_base64=False))
            data = msgpack.packb(items, use_bin_type=True)
        if content_encoding.is_gzip:
            data = gzip.compress(data)
        return data

    @classmethod
    def batch_decode(cls, data, content_encoding=None):
        content_encoding = ContentEncoding.of(content_encoding)
        if content_encoding.is_gzip:
            data = gzip.decompress(data)
        messages = []
        if content_encoding.is_json:
            data = json.loads(data.decode('utf-8'))
            for x in data:
                messages.append(cls._from_dict(x, do_base64=True))
        else:
            data = msgpack.unpackb(raw=False)
            for x in data:
                messages.append(cls._from_dict(x, do_base64=False))
        return messages
