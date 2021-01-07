"""
>>> k = HASH_ID.encode(123)
>>> len(k)
6
>>> HASH_ID.decode(k)[0]
123
"""
from hashids import Hashids
from rssant_config import CONFIG
from .unionid import UNION_ID_CHARS

HASH_ID = Hashids(
    salt=CONFIG.hashid_salt,
    min_length=6,
    alphabet=UNION_ID_CHARS.decode('utf-8'),
)
