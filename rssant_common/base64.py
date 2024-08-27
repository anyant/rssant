import base64


class UrlsafeBase64:

    @classmethod
    def encode(cls, data: bytes) -> str:
        if not data:
            return ''
        return base64.urlsafe_b64encode(data).decode('ascii')

    @classmethod
    def decode(cls, data: str) -> bytes:
        if not data:
            return b''
        return base64.urlsafe_b64decode(data)
