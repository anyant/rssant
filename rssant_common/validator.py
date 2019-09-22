import datetime
import time
import functools
from collections import namedtuple
from base64 import urlsafe_b64encode, urlsafe_b64decode

from validr import validator, SchemaError, Invalid, Compiler, builtin_validators
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .helper import coerce_url
from .cursor import Cursor
from .detail import detail_validator
from . import unionid


@validator(accept=(str, object), output=(str, object))
def cursor_validator(compiler, keys=None, output_object=False, base64=False):
    """Cursor: k1:v1,k2:v2"""
    if keys:
        try:
            keys = set(keys.strip().replace(',', ' ').split())
        except (TypeError, ValueError):
            raise SchemaError('invalid cursor keys')

    def validate(value):
        try:
            if not isinstance(value, Cursor):
                if base64:
                    value = urlsafe_b64decode(value.encode('ascii')).decode('utf-8')
                value = Cursor.from_string(value, keys)
            else:
                value._check_missing_keys(keys)
        except (UnicodeEncodeError, UnicodeDecodeError, ValueError) as ex:
            raise Invalid(str(ex)) from None
        if output_object:
            return value
        value = str(value)
        if base64:
            value = urlsafe_b64encode(value.encode('utf-8')).decode()
        return value

    return validate


@validator(accept=str, output=str)
def url_validator(compiler, schemes='http https', default_schema=None):
    """
    Args:
        default_schema: 接受没有scheme的url并尝试修正
    """
    schemes = set(schemes.replace(',', ' ').split(' '))
    _django_validate_url = URLValidator(schemes=schemes)
    if default_schema and default_schema not in schemes:
        raise SchemaError('invalid default_schema {}'.format(default_schema))

    def validate(value):
        if default_schema:
            value = coerce_url(value, default_schema=default_schema)
        try:
            _django_validate_url(value)
        except ValidationError:
            # TODO: access ValidationError.messages will cause error when
            # django/i18n not setup, maybe use validators package instead
            # raise Invalid(','.join(ex.messages).rstrip('.'))
            raise Invalid('invalid or incorrect url format')
        return value

    return validate


@validator(accept=(str, object), output=(str, object))
def datetime_validator(compiler, format='%Y-%m-%dT%H:%M:%S.%fZ', output_object=False):
    def validate(value):
        try:
            if isinstance(value, list) and len(value) == 9:
                value = tuple(value)
            if isinstance(value, tuple):
                value = datetime.datetime.fromtimestamp(time.mktime(value), tz=timezone.utc)
            elif not isinstance(value, datetime.datetime):
                value = parse_datetime(value)
                if value is None:
                    raise Invalid('not well formatted datetime')
            if not timezone.is_aware(value):
                value = timezone.make_aware(value, timezone=timezone.utc)
            # https://bugs.python.org/issue13305
            if value.year < 1000:
                raise Invalid('not support datetime before 1000-01-01')
            if output_object:
                return value
            else:
                return value.strftime(format)
        except Invalid:
            raise
        except Exception as ex:
            raise Invalid('invalid datetime') from ex
    return validate


FeedUnionId = namedtuple('FeedUnionId', 'user_id, feed_id')
StoryUnionId = namedtuple('StoryUnionId', 'user_id, feed_id, offset')


def create_unionid_validator(tuple_class):
    @validator(accept=(str, object), output=(str, object))
    def unionid_validator(compiler, output_object=False):
        def validate(value):
            try:
                if isinstance(value, str):
                    value = unionid.decode(value)
                if output_object:
                    return tuple_class(*value)
                else:
                    return unionid.encode(*value)
            except (unionid.UnionIdError, TypeError, ValueError) as ex:
                raise Invalid('invalid unionid, {}'.format(str(ex))) from ex
        return validate
    return unionid_validator


def dict_validator(compiler, schema):
    schema = schema.copy()
    remove_empty = schema.params.pop('remove_empty', False)
    origin_validate = builtin_validators['dict'](compiler, schema)

    def validate(value):
        value = origin_validate(value)
        if value and remove_empty:
            value = {k: v for k, v in value.items() if v is not None and v != ''}
        return value

    attrs = ['__schema__', '__module__', '__name__', '__qualname__']
    for k in attrs:
        setattr(validate, k, getattr(origin_validate, k, None))

    return validate


def str_validator(compiler, schema):
    """
    >>> from validr import T
    >>> f = compiler.compile(T.str.maxlen(10).truncated.strip)
    >>> f(" 123456789 x")
    '123456789'
    """
    schema = schema.copy()
    truncated = schema.params.pop('truncated', False)
    strip = schema.params.pop('strip', False)
    lstrip = schema.params.pop('lstrip', False)
    rstrip = schema.params.pop('rstrip', False)
    maxlen = int(schema.params.get('maxlen', 1024 * 1024))
    origin_validate = builtin_validators['str'](compiler, schema)

    @functools.wraps(origin_validate)
    def validate(value):
        if isinstance(value, int):
            value = str(value)
        if truncated and isinstance(value, str) and len(value) > maxlen:
            value = value[:maxlen]
        value = origin_validate(value)
        if value:
            if strip:
                value = value.strip()
            else:
                if lstrip:
                    value = value.lstrip()
                if rstrip:
                    value = value.rstrip()
        return value

    return validate


INTERVAL_UNITS = {'s': 1, 'm': 60, 'h': 60 * 60, 'd': 24 * 60 * 60}


def parse_interval(t) -> datetime.timedelta:
    if isinstance(t, datetime.timedelta):
        return t
    if isinstance(t, (int, float)):
        seconds = t
    else:
        seconds = int(t[:-1]) * INTERVAL_UNITS[t[-1]]
    return datetime.timedelta(seconds=seconds)


@validator(accept=str, output=object)
def interval_validator(compiler, min='0s', max='365d'):
    """Time interval validator, convert value to seconds

    Supported time units:
        s: seconds, eg: 10s
        m: minutes, eg: 10m
        h: hours, eg: 1h
        d: days, eg: 7d
    """
    try:
        min = parse_interval(min)
    except (IndexError, KeyError, ValueError):
        raise SchemaError('invalid min value') from None
    try:
        max = parse_interval(max)
    except (IndexError, KeyError, ValueError):
        raise SchemaError('invalid max value') from None

    def validate(value):
        try:
            value = parse_interval(value)
        except (IndexError, KeyError, ValueError):
            raise Invalid("invalid interval") from None
        if value < min:
            raise Invalid("interval must >= {}".format(min))
        if value > max:
            raise Invalid("interval must <= {}".format(max))
        return value
    return validate


@validator(accept=str, output=str)
def enum_validator(compiler, items):
    items = set(items.replace(',', ' ').split())

    def validate(value):
        if value in items:
            return value
        raise Invalid('value must be one of {}'.format(items))

    return validate


VALIDATORS = {
    'cursor': cursor_validator,
    'url': url_validator,
    'datetime': datetime_validator,
    'feed_unionid': create_unionid_validator(FeedUnionId),
    'story_unionid': create_unionid_validator(StoryUnionId),
    'detail': detail_validator,
    'dict': dict_validator,
    'str': str_validator,
    'interval': interval_validator,
    'enum': enum_validator,
}


compiler = Compiler(validators=VALIDATORS)
