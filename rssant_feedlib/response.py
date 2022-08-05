import enum
from http import HTTPStatus


class FeedResponseStatus(enum.IntEnum):
    # http://docs.python-requests.org/en/master/_modules/requests/exceptions/
    UNKNOWN_ERROR = -100
    CONNECTION_ERROR = -200
    PROXY_ERROR = -300
    RSS_PROXY_ERROR = -301
    RESPONSE_ERROR = -400
    DNS_ERROR = -201
    PRIVATE_ADDRESS_ERROR = -202
    CONNECTION_TIMEOUT = -203
    SSL_ERROR = -204
    READ_TIMEOUT = -205
    CONNECTION_RESET = -206
    TOO_MANY_REDIRECT_ERROR = -401
    CHUNKED_ENCODING_ERROR = -402
    CONTENT_DECODING_ERROR = -403
    CONTENT_TOO_LARGE_ERROR = -404
    REFERER_DENY = -405  # 严格防盗链，必须服务端才能绕过
    REFERER_NOT_ALLOWED = -406  # 普通防盗链，不带Referer头可绕过
    CONTENT_TYPE_NOT_SUPPORT_ERROR = -407  # 非文本/HTML响应

    @classmethod
    def name_of(cls, value):
        """
        >>> FeedResponseStatus.name_of(200)
        'OK'
        >>> FeedResponseStatus.name_of(-200)
        'FEED_CONNECTION_ERROR'
        >>> FeedResponseStatus.name_of(-999)
        'FEED_E999'
        """
        if value > 0:
            try:
                return HTTPStatus(value).name
            except ValueError:
                # eg: http://huanggua.sinaapp.com/
                # ValueError: 600 is not a valid HTTPStatus
                return f'HTTP_{value}'
        else:
            try:
                return 'FEED_' + FeedResponseStatus(value).name
            except ValueError:
                return f'FEED_E{abs(value)}'

    @classmethod
    def is_need_proxy(cls, value):
        """
        >>> FeedResponseStatus.is_need_proxy(FeedResponseStatus.CONNECTION_TIMEOUT)
        True
        >>> FeedResponseStatus.is_need_proxy(403)
        True
        >>> FeedResponseStatus.is_need_proxy(404)
        False
        """
        return value in _NEED_PROXY_STATUS_SET

    @classmethod
    def is_permanent_failure(cls, value):
        """
        >>> FeedResponseStatus.is_permanent_failure(FeedResponseStatus.CONNECTION_TIMEOUT)
        True
        >>> FeedResponseStatus.is_permanent_failure(FeedResponseStatus.RSS_PROXY_ERROR)
        True
        >>> FeedResponseStatus.is_permanent_failure(404)
        False
        """
        return value < 0 and abs(value) // 100 in (2, 3)

    @classmethod
    def is_referrer_deny(cls, value):
        """
        >>> FeedResponseStatus.is_referrer_deny(403)
        True
        >>> FeedResponseStatus.is_referrer_deny(500)
        False
        """
        return value in _IMAGE_REFERER_DENY_STATUS


_NEED_PROXY_STATUS_SET = {x.value for x in [
    FeedResponseStatus.CONNECTION_ERROR,
    FeedResponseStatus.DNS_ERROR,
    FeedResponseStatus.CONNECTION_TIMEOUT,
    FeedResponseStatus.READ_TIMEOUT,
    FeedResponseStatus.CONNECTION_RESET,
    FeedResponseStatus.PRIVATE_ADDRESS_ERROR,
    HTTPStatus.UNAUTHORIZED,
    HTTPStatus.FORBIDDEN,
]}


_IMAGE_REFERER_DENY_STATUS = set([
    400, 401, 403, 404,
    FeedResponseStatus.REFERER_DENY.value,
    FeedResponseStatus.REFERER_NOT_ALLOWED.value,
])


class FeedContentType(enum.Enum):

    HTML = 'HTML'
    JSON = 'JSON'
    XML = 'XML'
    OTHER = 'OTHER'

    def __repr__(self):
        return '<%s.%s>' % (self.__class__.__name__, self.name)

    @property
    def is_html(self) -> bool:
        return self == FeedContentType.HTML

    @property
    def is_xml(self) -> bool:
        return self == FeedContentType.XML

    @property
    def is_json(self) -> bool:
        return self == FeedContentType.JSON

    @property
    def is_other(self) -> bool:
        return self == FeedContentType.OTHER


class FeedResponse:

    __slots__ = (
        '_content',
        '_status',
        '_url',
        '_encoding',
        '_etag',
        '_last_modified',
        '_mime_type',
        '_feed_type',
        '_use_proxy',
    )

    def __init__(
        self, *,
        content: bytes = None,
        status: int = None,
        url: str = None,
        etag: str = None,
        last_modified: str = None,
        encoding: str = None,
        mime_type: str = None,
        feed_type: FeedContentType = None,
        use_proxy: bool = None,
    ):
        self._content = content
        self._status = int(status if status is not None else HTTPStatus.OK.value)
        self._url = url
        self._encoding = encoding
        self._etag = etag
        self._last_modified = last_modified
        self._mime_type = mime_type
        self._feed_type = feed_type or FeedContentType.OTHER
        self._use_proxy = use_proxy

    def __repr__(self):
        name = type(self).__name__
        length = len(self._content) if self._content else 0
        status_name = FeedResponseStatus.name_of(self.status)
        feed_type = self._feed_type.value if self._feed_type else None
        return (
            f'<{name} {self.status} {status_name} url={self.url!r} length={length} '
            f'encoding={self.encoding!r} feed_type={feed_type!r}>'
        )

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def status(self) -> int:
        return self._status

    @property
    def ok(self) -> bool:
        return self._status == HTTPStatus.OK.value

    @property
    def is_need_proxy(self) -> bool:
        return FeedResponseStatus.is_need_proxy(self._status)

    @property
    def url(self) -> str:
        return self._url

    @property
    def etag(self) -> str:
        return self._etag

    @property
    def last_modified(self) -> str:
        return self._last_modified

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def mime_type(self) -> str:
        return self._mime_type

    @property
    def feed_type(self) -> FeedContentType:
        return self._feed_type

    @property
    def use_proxy(self) -> bool:
        return self._use_proxy
