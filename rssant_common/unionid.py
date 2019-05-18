"""
UnionId Encode and Decode
"""

UNION_ID_CHARS = b'0123456789abcdefghkmnpqrstuvwxyz'
UNION_ID_CHARS_MAP = {c: i for i, c in enumerate(UNION_ID_CHARS)}

N2_MAX = 2 ** (2 * 5 - 3) - 1    # 2^7  - 1
N4_MAX = 2 ** (4 * 5 - 3) - 1    # 2^17 - 1
N7_MAX = 2 ** (7 * 5 - 3) - 1    # 2^32 - 1
N10_MAX = 2 ** (10 * 5 - 3) - 1  # 2^47 - 1
N13_MAX = 2 ** (13 * 5 - 1) - 1  # 2^64 - 1

N2_PREFIX = 0b00000000  # >>2 == 0
N4_PREFIX = 0b00000100  # >>2 == 1
N7_PREFIX = 0b00001000  # >>2 == 2
N10_PREFIX = 0b00001100  # >>2 == 3
N13_PREFIX = 0b00010000  # >>2 == 4


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
    if n <= N2_MAX:
        length = 2
    elif n <= N4_MAX:
        length = 4
    elif n <= N7_MAX:
        length = 7
    elif n <= N10_MAX:
        length = 10
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
    elif length == 10:
        data[0] = UNION_ID_CHARS[n | N10_PREFIX]
    elif length == 7:
        data[0] = UNION_ID_CHARS[n | N7_PREFIX]
    elif length == 4:
        data[0] = UNION_ID_CHARS[n | N4_PREFIX]
    else:
        data[0] = UNION_ID_CHARS[n | N2_PREFIX]
    return data


def encode(*numbers) -> bytes:
    if len(numbers) == 1 and not isinstance(numbers[0], int):
        numbers = numbers[0]
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
        else:
            m = v & 0b00001100
            if m == N10_PREFIX:
                length = 10
            elif m == N7_PREFIX:
                length = 7
            elif m == N4_PREFIX:
                length = 4
            else:
                length = 2
            v = (v & 0b00000011) << (5 * (length - 1))
        if i + length > data_length:
            raise UnionIdDecodeError("data length incorrect")
        ret.append(_decode_number(v, data[i + 1: i + length]))
        i += length
    return tuple(ret)


if __name__ == "__main__":
    import random
    number_maxs = [N2_MAX, N4_MAX, N7_MAX, N10_MAX, N13_MAX]
    small_numbers = list(range(N2_MAX * 2 + 1))
    max_plus_1 = [x + 1 for x in number_maxs[:-1]]
    max_sub_1 = [x - 1 for x in number_maxs]
    numbers = tuple(number_maxs + small_numbers + max_plus_1 + max_sub_1)
    numbers_reversed = tuple(reversed(numbers))
    numbers_random = list(numbers)
    random.shuffle(numbers_random)
    numbers_random = tuple(numbers_random)
    print('total {} numbers'.format(len(numbers)))

    def _debug_decode(data, expect):
        try:
            return decode(data)
        except UnionIdDecodeError:
            print('data={} expect={}'.format(data, expect))
            raise

    def test_n(n):
        for i in range(len(numbers) - n):
            nums = tuple(numbers[i:i + n])
            result = _debug_decode(encode(*nums), nums)
            assert result == nums, nums

    def test_numbers():
        result = _debug_decode(encode(*numbers), numbers)
        assert result == numbers, numbers

    def test_reversed():
        result = _debug_decode(encode(*numbers_reversed), numbers_reversed)
        assert result == numbers_reversed, numbers_reversed

    def test_random():
        result = _debug_decode(encode(*numbers_random), numbers_random)
        assert result == numbers_random, numbers_random

    import timeit
    for n in [1, 2, 3, 7, 13]:
        print(timeit.timeit(lambda: test_n(n), number=1000))
    print(timeit.timeit(test_numbers, number=10000))
    print(timeit.timeit(test_reversed, number=10000))
    print(timeit.timeit(test_random, number=10000))
