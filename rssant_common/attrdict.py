"""
Simplified https://github.com/bcj/AttrDict

>>> d = AttrDict(x=1)
>>> d.x == 1
True
>>> d.y = 2
>>> d.y == 2
True
>>> del d.y
>>> d.y
Traceback (most recent call last):
    ...
AttributeError: 'AttrDict' object has no attribute 'y'
>>> del d.y
Traceback (most recent call last):
    ...
AttributeError: 'AttrDict' object has no attribute 'y'
"""
from typing import Any


class AttrDict(dict):

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(_no_attr_msg(name)) from None

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError:
            raise AttributeError(_no_attr_msg(name)) from None


def _no_attr_msg(name: str) -> str:
    return f"{AttrDict.__name__!r} object has no attribute {name!r}"
