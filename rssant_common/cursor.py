

class Cursor:

    def __init__(self, **items):
        items = {k: v for k, v in items.items() if v is not None and v != ''}
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
