from datetime import datetime, date, time, timedelta, timezone  # noqa: F401


UTC = timezone.utc
CST = timezone(timedelta(hours=8), name='Asia/Shanghai')


def now() -> datetime:
    """
    >>> now().tzinfo == UTC
    True
    """
    return datetime.utcnow().replace(tzinfo=UTC)
