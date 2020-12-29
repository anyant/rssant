import typing
import os.path

from xml.sax.saxutils import escape as xml_escape
from xml.sax.saxutils import quoteattr as xml_quote
from mako.template import Template

from rssant.settings import BASE_DIR
from rssant_api.models import UnionFeed


_FEED_GROUP_NAME_MAP = {
    'SYS:SOLO': '无分组',
    'SYS:MUSHROOM': '品读',
}

_FEED_GROUP_ID_MAP = {v: k for k, v in _FEED_GROUP_NAME_MAP.items()}


def is_system_group(group) -> bool:
    """
    >>> is_system_group('SYS:SOLO')
    True
    >>> is_system_group('sys:mushroom')
    True
    >>> is_system_group(None)
    False
    """
    return bool(group and group.upper().startswith('SYS:'))


def group_name_of(group) -> str:
    """
    >>> group_name_of(None) is None
    True
    >>> group_name_of('') == ''
    True
    >>> group_name_of('sys:MUSHROOM')
    '品读'
    """
    return _FEED_GROUP_NAME_MAP.get((group or '').upper(), group)


def group_id_of(group) -> str:
    """
    >>> group_id_of(None) is None
    True
    >>> group_id_of('') == ''
    True
    >>> group_id_of('品读')
    'SYS:MUSHROOM'
    """
    return _FEED_GROUP_ID_MAP.get(group, group)


OPML_TEMPLATE_PATH = os.path.join(BASE_DIR, 'rssant_api', 'resources', 'opml.mako')


def render_opml(union_feeds: typing.List[UnionFeed]) -> str:
    """
    see also: http://dev.opml.org/spec2.html
    """
    union_feeds = list(sorted(union_feeds, key=lambda x: (x.group or '', x.url or '')))
    feed_items = []
    fields = ['title', 'url', 'version', 'group', 'origin_title', 'link']
    xml_keys = ['text', 'xmlUrl', 'type', 'category', 'title', 'htmlUrl']
    for feed in union_feeds:
        attrs = []
        for field, key in zip(fields, xml_keys):
            value = getattr(feed, field, None)
            if not value:
                continue
            if field == 'group' and is_system_group(value):
                continue
            value = xml_quote(xml_escape(value))
            attrs.append('{}={}'.format(key, value))
        feed_items.append(dict(attrs=' '.join(attrs)))
    tmpl = Template(filename=OPML_TEMPLATE_PATH)
    content = tmpl.render(feeds=feed_items)
    return content
