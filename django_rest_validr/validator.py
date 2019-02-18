from validr import T, validator, SchemaError, Invalid


def page_of(item):
    return T.dict(
        previous=T.cursor_string.optional,
        next=T.cursor_string.optional,
        size=T.int.optional,
        results=T.list(item)
    )


class Cursor:
    def __init__(self, **items):
        self.__dict__['_items'] = items

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
def cursor_validator(compiler, keys=None):
    """Cursor: k1:v1,k2:v2"""
    if keys:
        try:
            keys = set(keys.strip().replace(',', ' ').split())
        except (TypeError, ValueError):
            raise SchemaError('invalid cursor keys')

    def validate(value):
        pairs = value.strip().split(',')
        items = {}
        for p in pairs:
            kv = p.split(':', maxsplit=1)
            if len(kv) != 2:
                raise Invalid(f'invalid cursor segment {p!r}')
            key = kv[0]
            if keys is not None and key not in keys:
                raise Invalid(f'invalid cursor key {key!r}')
            items[key] = kv[1]
        if keys is not None:
            missing_keys = ','.join(keys - set(items))
            if missing_keys:
                raise Invalid(f'missing cursor keys {{{missing_keys}}}')
        return Cursor(**items)

    return validate


@validator(string=False)
def cursor_string_validator(compiler):
    def validate(value):
        if not isinstance(value, Cursor):
            raise Invalid('invalid cursor')
        return str(value)
    return validate


VALIDATORS = {
    'cursor': cursor_validator,
    'cursor_string': cursor_string_validator,
}
