from datetime import date, datetime, time, timedelta, timezone  # noqa: F401

UTC = timezone.utc
CST = timezone(timedelta(hours=8), name='Asia/Shanghai')


def now() -> datetime:
    """
    >>> now().tzinfo == UTC
    True
    """
    return datetime.now(timezone.utc)
