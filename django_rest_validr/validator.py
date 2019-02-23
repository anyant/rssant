from validr import T, validator, SchemaError, Invalid, builtin_validators

from feedlib.helper import coerce_url


def pagination(item):
    return T.dict(
        previous=T.cursor.optional,
        next=T.cursor.optional,
        total=T.int.optional,
        size=T.int.optional,
        results=T.list(item)
    )


class Cursor:

    def __init__(self, **items):
        self.__dict__['_items'] = items

    def _check_missing_keys(self, keys):
        if keys is not None:
            missing_keys = ','.join(keys - set(self._items))
            if len(missing_keys) == 1:
                raise ValueError(f'missing cursor key {list(missing_keys)[0]}')
            elif missing_keys:
                raise ValueError(f'missing cursor keys {{{missing_keys}}}')

    @staticmethod
    def from_string(value, keys=None):
        try:
            pairs = value.strip().split(',')
        except AttributeError:
            raise ValueError('invalid cursor')
        items = {}
        for p in pairs:
            kv = p.split(':', maxsplit=1)
            if len(kv) != 2:
                raise ValueError(f'invalid cursor segment {p!r}')
            key = kv[0]
            if keys is not None and key not in keys:
                raise ValueError(f'invalid cursor key {key!r}')
            items[key] = kv[1]
        cursor = Cursor(**items)
        cursor._check_missing_keys(keys)
        return cursor

    def __str__(self):
        return ','.join([f'{k}:{v}' for k, v in self._items.items()])

    def __repr__(self):
        return f'<Cursor {self}>'

    def __getattr__(self, key):
        return self._items[key]

    def __setattr__(self, key, value):
        self._items[key] = value

    def __getitem__(self, key):
        return self._items[key]

    def __setitem__(self, key, value):
        self._items[key] = value


@validator(string=False)
def cursor_validator(compiler, keys=None, object=False):
    """Cursor: k1:v1,k2:v2"""
    if keys:
        try:
            keys = set(keys.strip().replace(',', ' ').split())
        except (TypeError, ValueError):
            raise SchemaError('invalid cursor keys')
    return_object = object

    def validate(value):
        try:
            if not isinstance(value, Cursor):
                value = Cursor.from_string(value, keys)
            else:
                value._check_missing_keys(keys)
        except ValueError as ex:
            raise Invalid(str(ex))
        if return_object:
            return value
        else:
            return str(value)

    return validate


@validator(string=True)
def url_validator(*args, tolerant=False, **kwargs):
    """
    Args:
        tolerant: 接受没有scheme的url并尝试修正
    """
    _validate_url = builtin_validators['url'].validator(*args, **kwargs)

    def validate(value):
        if tolerant:
            value = coerce_url(value)
        return _validate_url(value)

    return validate


VALIDATORS = {
    'cursor': cursor_validator,
    'url': url_validator,
}
