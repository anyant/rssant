"""
UnionId Encode and Decode
"""

UNION_ID_CHARS = b'0123456789abcdefghkmnpqrstuvwxyz'
UNION_ID_CHARS_MAP = {c: i for i, c in enumerate(UNION_ID_CHARS)}

N4_MAX = 2 ** (4 * 5 - 2) - 1
N7_MAX = 2 ** (7 * 5 - 2) - 1
N13_MAX = 2 ** (13 * 5 - 1) - 1

N7_PREFIX = 0b00001000
N13_PREFIX = 0b00010000


class UnionIdError(Exception):
    """UnionId Error"""


class UnionIdEncodeError(UnionIdError):
    """UnionId Encode Error"""


class UnionIdDecodeError(UnionIdError):
    """UnionId Decode Error"""


def _value_of_char(char):
    try:
        return UNION_ID_CHARS_MAP[char]
    except KeyError:
        raise UnionIdDecodeError("invalid character '{}'".format(chr(char))) from None


def _encode_number(n):
    if n < 0:
        raise UnionIdEncodeError('negative number not allowed')
    if n <= N4_MAX:
        length = 4
    elif n <= N7_MAX:
        length = 7
    elif n <= N13_MAX:
        length = 13
    else:
        raise UnionIdEncodeError('number too large, expect number < 2**64')
    data = bytearray(length)
    for i in range(1, length):
        data[-i] = UNION_ID_CHARS[n & 0b00011111]
        n = n >> 5
    if length == 13:
        data[0] = UNION_ID_CHARS[n | N13_PREFIX]
    elif length == 7:
        data[0] = UNION_ID_CHARS[n | N7_PREFIX]
    else:
        data[0] = UNION_ID_CHARS[n]
    return data


def encode(*numbers) -> bytes:
    data = bytearray()
    for n in numbers:
        data += _encode_number(n)
    return data.decode('ASCII')


def _decode_number(value, data):
    for i, char in enumerate(reversed(data)):
        v = _value_of_char(char)
        value |= v << (i * 5)
    return value


def decode(data) -> tuple:
    data = memoryview(data.encode('ASCII'))
    data_length = len(data)
    i = 0
    ret = []
    while i < data_length:
        v = _value_of_char(data[i])
        if v & N13_PREFIX:
            length = 13
            v = (v & 0b00001111) << 60
        elif v & N7_PREFIX:
            length = 7
            v = (v & 0b00000111) << 30
        else:
            length = 4
            v = (v & 0b00000111) << 15
        if i + length > data_length:
            raise UnionIdDecodeError("data length incorrect")
        ret.append(_decode_number(v, data[i + 1: i + length]))
        i += length
    return tuple(ret)


if __name__ == "__main__":
    def test_4():
        numbers = (0, 2**17 - 1)
        result = decode(encode(*numbers))
        assert result == numbers, numbers

    def test_7():
        numbers = (2**18 + 1, 2**32 - 1)
        result = decode(encode(*numbers))
        assert result == numbers, numbers

    def test_13():
        numbers = (2**33 + 1, 2**63 - 1)
        result = decode(encode(*numbers))
        assert result == numbers, numbers

    import timeit
    print(timeit.timeit(test_4, number=100000))
    print(timeit.timeit(test_7, number=100000))
    print(timeit.timeit(test_13, number=100000))
