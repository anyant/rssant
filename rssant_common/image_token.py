import time
import base64
import json
import hmac
import brotli
from validr import T, Invalid, Compiler


validate_image_token = Compiler().compile(T.dict(
    timestamp=T.int,
    referrer=T.url.optional,
    owner=T.str.optional,
))


class ImageTokenEncodeError(Exception):
    """ImageTokenEncodeError"""


class ImageTokenDecodeError(Exception):
    """ImageTokenDecodeError"""


class ImageTokenExpiredError(ImageTokenDecodeError):
    """ImageTokenExpiredError"""


class ImageToken:

    def __init__(
        self, *,
        referrer: str = None,
        timestamp: int = None,
        owner: str = None,
    ):
        self.referrer = (referrer or '')[:255]
        self.timestamp = timestamp or int(time.time())
        self.owner = owner or ''

    def __repr__(self):
        return '<{} referrer={!r} owner={!r} @{}>'.format(
            type(self).__name__, self.referrer, self.owner, self.timestamp)

    @staticmethod
    def _b64encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode()

    @staticmethod
    def _b64decode(text: str) -> bytes:
        return base64.urlsafe_b64decode(text)

    @classmethod
    def _encode_payload(cls, payload: dict) -> str:
        text = json.dumps(payload, ensure_ascii=False)
        data = brotli.compress(text.encode('utf-8'), quality=6)
        return cls._b64encode(data)

    @classmethod
    def _decode_payload(cls, text: str) -> dict:
        data = brotli.decompress(cls._b64decode(text))
        return json.loads(data.decode('utf-8'))

    @classmethod
    def _sign(cls, text: str, secret: str) -> str:
        sign = hmac.digest(secret.encode('utf-8'), text.encode('utf-8'), 'md5')
        return cls._b64encode(sign[:12])

    def encode(self, secret: str) -> str:
        try:
            payload = validate_image_token(dict(
                referrer=self.referrer,
                timestamp=self.timestamp,
                owner=self.owner,
            ))
            text = self._encode_payload(payload)
            return text + '.' + self._sign(text, secret=secret)
        except (Invalid, json.JSONDecodeError, brotli.error, UnicodeEncodeError) as ex:
            raise ImageTokenEncodeError(str(ex)) from ex

    @classmethod
    def decode(cls, token: str, secret: str, expires: int = None, clock=time.time) -> 'ImageToken':
        parts = token.split('.', 1)
        if len(parts) != 2:
            raise ImageTokenDecodeError('invalid image token format')
        text, sign = parts
        expect_sign = cls._sign(text, secret=secret)
        if sign != expect_sign:
            raise ImageTokenDecodeError('image token signature mismatch')
        try:
            payload = cls._decode_payload(text)
            payload = validate_image_token(payload)
        except (Invalid, json.JSONDecodeError, brotli.error, UnicodeDecodeError) as ex:
            raise ImageTokenDecodeError(str(ex)) from ex
        if expires is not None and expires > 0:
            is_expired = int(clock()) - payload['timestamp'] >= expires
            if is_expired:
                raise ImageTokenExpiredError('image token expired')
        return ImageToken(**payload)
