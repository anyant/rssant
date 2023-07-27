import datetime
import functools
from collections import namedtuple
from base64 import urlsafe_b64encode, urlsafe_b64decode
from urllib.parse import urlparse

from validr import T, validator, SchemaError, Invalid, Compiler, builtin_validators
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from rssant.settings import ENV_CONFIG

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
        except (UnicodeError, ValueError) as ex:
            raise Invalid(str(ex)) from None
        if output_object:
            return value
        value = str(value)
        if base64:
            value = urlsafe_b64encode(value.encode('utf-8')).decode()
        return value

    return validate


@validator(accept=str, output=str)
def url_validator(compiler, scheme='http https', default_schema=None, maxlen=1024, relaxed=False):
    """
    Args:
        default_schema: 接受没有scheme的url并尝试修正
        relaxed: accept not strict url
    """
    if relaxed:
        return Compiler().compile(T.url.maxlen(maxlen).scheme(scheme))
    schemes = set(scheme.replace(',', ' ').split(' '))
    if default_schema and default_schema not in schemes:
        raise SchemaError('invalid default_schema {}'.format(default_schema))
    _django_validate_url = URLValidator(schemes=schemes)

    def _is_in_hosts(domain):
        file_path = '/etc/hosts'
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    _, *domains = parts
                    if domain in domains:
                        return True
        return False

    def validate(value):
        if default_schema:
            value = coerce_url(value, default_schema=default_schema)
        try:
            _django_validate_url(value)
        except ValidationError:
            # TODO: access ValidationError.messages will cause error when
            # django/i18n not setup, maybe use validators package instead
            # raise Invalid(','.join(ex.messages).rstrip('.'))
            if ENV_CONFIG.allow_private_address:
                domain = urlparse(value).netloc.split(':')[0]
                if '.' not in domain and _is_in_hosts(domain):
                    return value
            raise Invalid('invalid or incorrect url format')
        if len(value) > maxlen:
            raise Invalid(f'url length must <= {maxlen}')
        return value

    return validate


@validator(accept=(str, object), output=(str, object))
def datetime_validator(compiler, format='%Y-%m-%dT%H:%M:%S.%fZ', output_object=False):
    def validate(value):
        try:
            if not isinstance(value, datetime.datetime):
                value = parse_datetime(value)
                if value is None:
                    raise Invalid('not well formatted datetime')
            if not timezone.is_aware(value):
                value = timezone.make_aware(value, timezone=timezone.utc)
            # https://bugs.python.org/issue13305
            if value.year < 1000:
                raise Invalid('not support datetime before year 1000')
            if value.year > 2999:
                raise Invalid('not support datetime after year 2999')
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


@validator(accept=bytes, output=bytes)
def bytes_validator(compiler, maxlen=None):

    def validate(value):
        if not isinstance(value, bytes):
            raise Invalid('invalid bytes type')
        if maxlen is not None:
            if len(value) > maxlen:
                raise Invalid('value must <= {}'.format(maxlen))
        return value

    return validate


VALIDATORS = {
    'cursor': cursor_validator,
    'url': url_validator,
    'datetime': datetime_validator,
    'feed_unionid': create_unionid_validator(FeedUnionId),
    'story_unionid': create_unionid_validator(StoryUnionId),
    'detail': detail_validator,
    'str': str_validator,
    'bytes': bytes_validator,
}


compiler = Compiler(validators=VALIDATORS)

# warming up django url validator
# compiler.compile(T.url)('https://example.com/')
