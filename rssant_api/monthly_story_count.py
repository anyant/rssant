import typing
import logging
import struct
import math


LOG = logging.getLogger(__name__)


def month_of_id(month_id) -> typing.Tuple[int, int]:
    """
    >>> month_of_id(0)
    (1970, 1)
    >>> month_of_id(10)
    (1970, 11)
    >>> month_of_id(12)
    (1971, 1)
    >>> month_of_id(13)
    (1971, 2)
    >>> month_of_id(130)
    (1980, 11)
    """
    if month_id < 0:
        raise ValueError(f'invalid month_id {month_id}')
    n, m = divmod(month_id, 12)
    return n + 1970, m + 1


def id_of_month(year, month) -> int:
    """
    >>> id_of_month(1970, 1)
    0
    >>> id_of_month(1970, 12)
    11
    >>> id_of_month(1971, 1)
    12
    >>> id_of_month(1980, 11)
    130
    >>> for i in range(1000, 2000):
    ...     assert id_of_month(*month_of_id(i)) == i
    """
    _check_year_month(year, month)
    return (year - 1970) * 12 + month - 1


def _check_year_month(year, month):
    if not (1970 <= year <= 9999):
        raise ValueError(f'year must between 1970 and 9999')
    if not (1 <= month <= 12):
        raise ValueError(f'month must between 1 and 12')


MAX_DRYNESS = 1000


def dryness_formula(days: int, num_storys: int) -> int:
    """
    Dryness(0~1000)

    >>> assert dryness_formula(30, 255) == 0, '255 storys per month'
    >>> assert dryness_formula(30, 127) == 125, '127 storys per month'
    >>> assert dryness_formula(30, 63) == 250, '63 storys per month'
    >>> assert dryness_formula(30, 31) == 375, '31 storys per month'
    >>> assert dryness_formula(30, 15) == 500, '15 storys per month'
    >>> assert dryness_formula(30, 7) == 625, '7 storys per month'
    >>> assert dryness_formula(30, 3) == 750, '3 storys per month'
    >>> assert dryness_formula(30, 1) == 875, '1 storys per month'
    >>> assert dryness_formula(30, 0) == 1000, '0 storys per month'
    >>> assert dryness_formula(1, 0) == 1000, '0 storys per day'
    >>> assert dryness_formula(0, 0) == 1000, '0 storys'
    """
    v = math.log2(256 / (num_storys / (days + 1) * 31 + 1))
    return max(0, min(1000, round(v / 8 * 1000)))


def _dryness_formula_test():
    items = [
        (30, 255, '255 storys per month'),
        (30, 127, '127 storys per month'),
        (30, 63, '63 storys per month'),
        (30, 31, '31 storys per month'),
        (30, 15, '15 storys per month'),
        (30, 7, '7 storys per month'),
        (30, 3, '3 storys per month'),
        (30, 1, '1 storys per month'),
        (30, 0, '0 storys per month'),
        (1, 0, '0 storys per day'),
        (0, 0, '0 storys'),
        (1, 1, 'daily'),
        (7, 1, 'weekly'),
        (365, 12, 'monthly'),
        (365, 1, 'yearly'),
    ]
    for days, num_story, desc in items:
        dryness = dryness_formula(days, num_story)
        print(f'>>> assert dryness_formula({days}, {num_story}) == {dryness}, {desc!r}')


class MonthlyStoryCount:
    """
    Data format, big-endian numbers, maximum 256 months, maximum 514 bytes data.
    +-------------------+--------------------+------------+
    |         16        |         16 * num_months         |
    +-------------------+--------------------+------------+
    | month_id_base(16) | month_id_offset(8) |  count(8)  |
    +-------------------+--------------------+------------+

    >>> x = MonthlyStoryCount()
    >>> bool(x)
    False
    >>> x
    <MonthlyStoryCount NA>
    >>> x.put(1970, 1, 0)
    >>> x.put(1970, 12, 255)
    >>> x.put(1971, 1, 10)
    >>> x.get(1970, 12)
    255
    >>> bool(x)
    True
    >>> print(str(x))
    197001:0,197012:255,197101:10
    >>> x
    <MonthlyStoryCount 197001:0,197012:255,197101:10>
    >>> x.put(1991, 0, 0)
    Traceback (most recent call last):
    ...
    ValueError: month must between 1 and 12
    >>> x = MonthlyStoryCount.load(x.dump())
    >>> x.put(1991, 6, 5)
    >>> x.put(1991, 7, 6)
    >>> x = MonthlyStoryCount.load(x.dump())
    >>> for year, month, count in x: print(year, month, count)
    1970 12 255
    1971 1 10
    1991 6 5
    1991 7 6
    >>> x = MonthlyStoryCount()
    >>> for i in range(300):
    ...     year, month = month_of_id(1234 + i)
    ...     x.put(year, month, i)
    >>> len(x.dump())
    514
    """

    def __init__(self, items: typing.List[typing.Tuple[int, int, int]] = None):
        self._data = {}
        if items:
            for year, month, count in items:
                self.put(year, month, count)

    def __str__(self):
        lines = [f'{year:04d}{month:02d}:{count}' for year, month, count in self]
        return ','.join(lines)

    def __repr__(self):
        content = str(self) or 'NA'
        return f'<{type(self).__name__} {content}>'

    def __bool__(self):
        return bool(self._data)

    @staticmethod
    def _average_dryness(month_id_dryness_s):
        """
        >>> y = [500, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 500]
        >>> len(y)
        12
        >>> MonthlyStoryCount._average_dryness(list(zip(range(12), y)))
        667
        """
        if not month_id_dryness_s:
            return MAX_DRYNESS
        # exponential moving average, modified on single value series
        alpha = 2 / 3
        t, s = month_id_dryness_s[0]
        for month_id, dryness in month_id_dryness_s[1:]:
            for _ in range(t + 1, month_id):
                s = alpha * MAX_DRYNESS + (1 - alpha) * s
            s = alpha * dryness + (1 - alpha) * s
            t = month_id
        return round(s)

    def dryness(self):
        """
        compute dryness over historys.

        >>> x = MonthlyStoryCount()
        >>> x.put(2019, 1, 1)
        >>> x.dryness()
        875
        >>> x.put(2019, 1, 15)
        >>> x.put(2019, 2, 15)
        >>> x.dryness()
        500
        >>> x = MonthlyStoryCount()
        >>> x.put(2019, 1, 15)
        >>> x.put(2019, 12, 15)
        >>> x.dryness()
        500
        """
        month_id_dryness_s = []
        for year, month, count in self:
            month_id = id_of_month(year, month)
            dryness = dryness_formula(30, count)
            month_id_dryness_s.append((month_id, dryness))
        dryness_1 = self._average_dryness(month_id_dryness_s)
        dryness_2 = self._average_dryness(month_id_dryness_s[1:])
        dryness_3 = self._average_dryness(month_id_dryness_s[:-1])
        return min([dryness_1, dryness_2, dryness_3])

    @classmethod
    def load(cls, data: bytes):
        if not data:
            return cls()
        if len(data) % 2 != 0:
            raise ValueError('invalid data, length mismatch')
        month_id_base = struct.unpack('>H', data[:2])[0]
        items = []
        for offset, count in struct.iter_unpack('>2B', data[2:]):
            year, month = month_of_id(month_id_base + offset)
            items.append((year, month, count))
        return cls(items)

    def dump(self) -> bytes:
        items = list(self)
        if not items:
            return b''
        min_year, min_month, __ = items[0]
        max_year, max_month, __ = items[-1]
        min_month_id = id_of_month(min_year, min_month)
        max_month_id = id_of_month(max_year, max_month)
        month_id_base = max(min_month_id, max_month_id - 255)
        buffer = bytearray(struct.pack('>H', month_id_base))
        for year, month, count in items:
            month_id = id_of_month(year, month)
            if month_id < month_id_base:
                continue
            month_id_offset = month_id - month_id_base
            buffer.extend(struct.pack('>2B', month_id_offset, min(255, count)))
        return bytes(buffer)

    def get(self, year, month) -> int:
        _check_year_month(year, month)
        return self._data.get((year, month), 0)

    def put(self, year, month, count) -> None:
        _check_year_month(year, month)
        if count < 0:
            raise ValueError('count must >= 0')
        self._data[(year, month)] = min(255, count)

    def __iter__(self) -> typing.Iterable[typing.Tuple[int, int, int]]:
        for (year, month), count in sorted(self._data.items()):
            yield year, month, count
