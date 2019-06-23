"""
>>> url = 'https://static.darmau.com/2019/06/figma.jpg'
>>> referer = 'https://www.darmau.com/figma-for-design-system/'
>>> encoded = encode_image_url(url, referer)
>>> decoded = decode_image_url(encoded)
>>> decoded['url'] == url
True
>>> decoded['referer'] == referer
True
"""
import base64
import json
import brotli
from validr import T, Invalid
from rssant_common.validator import compiler


validate_image_url = compiler.compile(T.dict(
    url=T.url,
    referer=T.url.optional,
))


class ImageUrlEncodeError(Exception):
    """ImageUrlEncodeError"""


class ImageUrlDecodeError(Exception):
    """ImageUrlDecodeError"""


def encode_image_url(url, referer=None):
    try:
        text = json.dumps(validate_image_url(dict(url=url, referer=referer)))
        data = brotli.compress(text.encode('utf-8'))
        return base64.urlsafe_b64encode(data).decode()
    except (Invalid, json.JSONDecodeError, brotli.error, UnicodeEncodeError) as ex:
        raise ImageUrlEncodeError(str(ex)) from ex


def decode_image_url(data):
    try:
        data = base64.urlsafe_b64decode(data)
        text = brotli.decompress(data).decode('utf-8')
        return validate_image_url(json.loads(text))
    except (Invalid, json.JSONDecodeError, brotli.error, UnicodeDecodeError) as ex:
        raise ImageUrlDecodeError(str(ex)) from ex


if __name__ == "__main__":
    url = 'https://static.darmau.com/2019/06/figma.jpg'
    referer = 'https://www.darmau.com/figma-for-design-system/'
    encoded = encode_image_url(url, referer)
    print(encoded)
    decoded = decode_image_url(encoded)
    assert decoded['url'] == url, decoded
    assert decoded['referer'] == referer, decoded
