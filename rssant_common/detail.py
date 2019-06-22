"""
请求中detail参数的解析和处理，用于描述需要/不需要的字段

字段分3类:
    fixed: 固定字段，任何情况都会返回的字段
    default: 默认 (detail=false) 返回字段
    extra: 其他字段，detail=true 时返回

detail值:
    -: 只返回固定字段
    true|1: 返回所有字段
    false|0: 返回固定字段和默认字段
    field1,field2...: 只返回固定字段和field1,field2...
    +field1,field2!field3,field4...: 在固定字段和默认字段基础上，增加field1,field2，去掉field3,field4...

>>> from validr import T, Compiler
>>> compiler = Compiler(validators=dict(detail=detail_validator))
>>> validate = compiler.compile(T.detail.fields('f1,f2').extra_fields('f3'))
>>> all_fields = {'f1', 'f2', 'f3'}
>>> validate('true') == Detail(True, exclude_fields=set(), all_fields=all_fields)
True
>>> validate(False) == Detail(False, exclude_fields={'f3'}, all_fields=all_fields)
True
>>> validate('f2,f3') == Detail(True, exclude_fields={'f1'}, all_fields=all_fields)
True
>>> validate('+f3!f1') == Detail(True, exclude_fields={'f1'}, all_fields=all_fields)
True
>>> validate('f1+f2')
Traceback (most recent call last):
...
validr._exception.Invalid: invalid detail value, value=f1+f2
"""
import re
from validr import Invalid, SchemaError, validator


class Detail:
    """
    >>> Detail(True, exclude_fields={'field1'})
    <Detail true !field1>
    >>> Detail(True, exclude_fields={'field1'}, all_fields={'field1', 'field2'})
    <Detail true +field2 !field1>
    >>> bool(Detail(True, exclude_fields={'a'}))
    True
    >>> Detail(True, exclude_fields={'a', 'b'}) == Detail(True, exclude_fields={'b', 'a'})
    True
    """

    def __init__(self, detail, *, exclude_fields, all_fields=None):
        self._detail = detail
        self._exclude_fields = set(exclude_fields)
        self._all_fields = set(all_fields) if all_fields else set()

    def __bool__(self):
        return self._detail

    def __repr__(self):
        detail = 'true' if self._detail else 'false'
        include_fields = ','.join(self.include_fields)
        exclude_fields = ','.join(self.exclude_fields)
        include_fields = f'+{include_fields} ' if include_fields else ''
        exclude_fields = f'!{exclude_fields}' if exclude_fields else ''
        return f'<{type(self).__name__} {detail} {include_fields}{exclude_fields}>'

    def __eq__(self, value):
        if not isinstance(value, Detail):
            return False
        return value._detail == self._detail and \
            value._exclude_fields == self._exclude_fields and \
            value._all_fields == self._all_fields

    @property
    def exclude_fields(self):
        return self._exclude_fields

    @property
    def include_fields(self):
        return self._all_fields - self._exclude_fields

    @property
    def all_fields(self):
        return self._all_fields

    @classmethod
    def from_schema(cls, value, schema):
        if isinstance(value, cls):
            return value
        if hasattr(schema, '__schema__'):
            schema = schema.__schema__
        fields, extra_fields = _parse_fields(
            schema.params.get('fields'), schema.params.get('extra_fields'))
        all_fields = fields | extra_fields
        if value:
            return Detail(True, exclude_fields=set(), all_fields=all_fields)
        else:
            return Detail(False, exclude_fields=extra_fields, all_fields=all_fields)


class InvalidDetailValue(Exception):
    """InvalidDetailValue"""


RE_DETAIL = re.compile(
    r'(true|1)|'
    r'(false|0)|'
    r'([^,!\+\s]+(?:,[^,!\+\s]+)*)|'
    r'(?:'
    r'([!\+][^,!\+\s]+(?:,[^,!\+\s]+)*)'
    r'([!\+][^,!\+\s]+(?:,[^,!\+\s]+)*)?'
    r')', re.I)


def _parse_detail_value(text):
    """
    >>> _parse_detail_value('true')
    (True, None, None, None)
    >>> _parse_detail_value('False')
    (False, None, None, None)
    >>> _parse_detail_value('f1,f2')
    (None, ['f1', 'f2'], None, None)
    >>> _parse_detail_value('+f1,f2')
    (None, None, ['f1', 'f2'], None)
    >>> _parse_detail_value('!f1,f2')
    (None, None, None, ['f1', 'f2'])
    >>> _parse_detail_value('+f1,f2!f3')
    (None, None, ['f1', 'f2'], ['f3'])
    >>> _parse_detail_value('a,b+c')
    Traceback (most recent call last):
    ...
    rssant_common.detail.InvalidDetailValue: invalid detail value
    """
    try:
        match = RE_DETAIL.fullmatch(text.strip())
    except (TypeError, AttributeError):
        raise InvalidDetailValue('invalid detail value') from None
    if not match:
        raise InvalidDetailValue('invalid detail value')
    t_true, t_false, t_select, t_extra_1, t_extra_2 = match.groups()
    if t_true:
        return True, None, None, None
    if t_false:
        return False, None, None, None
    if t_select:
        if t_select == '-':
            select_fields = []
        else:
            select_fields = list(t_select.split(','))
        return None, select_fields, None, None
    extra_fields = {}
    if t_extra_1 and t_extra_2 and t_extra_1[0] == t_extra_2[0]:
        raise InvalidDetailValue('invalid detail value')
    for t_extra in [t_extra_1, t_extra_2]:
        if t_extra:
            extra_fields[t_extra[0]] = list(t_extra[1:].split(','))
    return None, None, extra_fields.get('+'), extra_fields.get('!')


def _parse_fields(fields, extra_fields):
    """
    >>> fields, extra_fields = _parse_fields('a,b', ''' c d
    ...     e''')
    >>> fields == {'a', 'b'}
    True
    >>> extra_fields == {'c', 'd', 'e'}
    True
    >>> _parse_fields('a,b', 'b,c,d')
    Traceback (most recent call last):
    ...
    validr._exception.SchemaError: duplicated fields b
    """
    def _parse_one(text, error_message):
        if not text:
            return set()
        try:
            text = text.strip()
        except (TypeError, AttributeError):
            raise SchemaError(error_message) from None
        return set(text.replace(',', ' ').split())
    fields = _parse_one(fields, 'invalid fields')
    extra_fields = _parse_one(extra_fields, 'invalid extra_fields')
    if (not fields) and (not extra_fields):
        raise SchemaError("no fields provided")
    duplicated_fields = ','.join(fields & extra_fields)
    if duplicated_fields:
        raise SchemaError(f"duplicated fields {duplicated_fields}")
    return fields, extra_fields


@validator(accept=(str, object), output=object)
def detail_validator(compiler, items=None, fields=None, extra_fields=None):
    fields, extra_fields = _parse_fields(fields, extra_fields)
    all_fields = fields | extra_fields

    def validate(value):
        if value is True:
            return Detail(True, exclude_fields=set(), all_fields=all_fields)
        if value is False:
            return Detail(False, exclude_fields=extra_fields, all_fields=all_fields)
        try:
            true_false, select_fields_list, include_fields_list, exclude_fields_list = \
                _parse_detail_value(value)
        except InvalidDetailValue as ex:
            raise Invalid(str(ex)) from None
        if true_false is True:
            return Detail(True, exclude_fields=set(), all_fields=all_fields)
        if true_false is False:
            return Detail(False, exclude_fields=extra_fields, all_fields=all_fields)
        if select_fields_list is not None:
            exclude_fields = all_fields - set(select_fields_list)
        else:
            exclude_fields = set(extra_fields)
            if include_fields_list is not None:
                exclude_fields -= set(include_fields_list)
            if exclude_fields_list is not None:
                exclude_fields |= all_fields & set(exclude_fields_list)
        detail = not extra_fields.issubset(exclude_fields)
        return Detail(detail, exclude_fields=exclude_fields, all_fields=all_fields)

    return validate
