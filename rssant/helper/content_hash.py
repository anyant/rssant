import base64
import hashlib

from rssant.settings import RSSANT_CONTENT_HASH_METHOD


def compute_hash(*fields):
    """bytes -> bytes"""
    h = hashlib.new(RSSANT_CONTENT_HASH_METHOD)
    for content in fields:
        if isinstance(content, str):
            content = content.encode('utf-8')
        h.update(content or b'')
    return h.digest()


def compute_hash_base64(*fields):
    """bytes -> base64 string"""
    value = compute_hash(*fields)
    return base64.b64encode(value).decode()
