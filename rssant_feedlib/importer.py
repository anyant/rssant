import re
import io
import typing
import logging
from pathlib import Path
from urllib.parse import urlsplit

import yarl
import listparser
from validr import T, Invalid

from rssant_common.helper import coerce_url
from rssant_common.validator import compiler
from rssant_common.blacklist import compile_url_blacklist
from .schema import validate_opml, IMPORT_ITEMS_LIMIT
from .helper import RE_URL


LOG = logging.getLogger(__name__)

_RE_OPML_FILENAME = re.compile(r'^.*\.(opml|xml)$', re.I)
_validate_url = compiler.compile(T.url)


_BLACKLIST_CONTENT = """
youtube.com
facebook.com
amazon.com
wikipedia.org
twitter.com
vk.com
instagram.com
live.com
tmall.com
baidu.com
taobao.com
jd.com
qq.com
sohu.com
sina.com.cn
jd.com
weibo.com
360.cn
yandex.ru
netflix.com
linkedin.com
twitch.tv
list.tmall.com
t.co
pornhub.com
alipay.com
xvideos.com
yahoo.co.jp
ebay.com
microsoft.com
bing.com
ok.ru
imgur.com
bongacams.com
hao123.com
aliexpress.com
mail.ru
whatsapp.com
xhamster.com
xnxx.com
Naver.com
sogou.com
samsung.com
accuweather.com
goo.gl
sm.cn
meituan.com
dianping.com
qunar.com
ctrip.com
readthedocs.io
readthedocs.org
blog.csdn.net
toutiao.com
"""


_is_in_url_blacklist = compile_url_blacklist(_BLACKLIST_CONTENT)


def _load_dotwhat_blacklist() -> set:
    """
    http://dotwhat.net/
    """
    blacklist = set()
    data_dir = Path(__file__).parent / 'dotwhat_data'
    for filepath in data_dir.glob('*.txt'):
        lines = filepath.read_text().strip().splitlines()
        for line in lines:
            file_ext = line.strip().split('-', 1)[0]
            file_ext = file_ext.strip('.').strip().lower()
            blacklist.add(file_ext)
    return blacklist


_DOTWHAT_BLACKLIST = _load_dotwhat_blacklist()


def _is_in_blacklist(url: str):
    if _is_in_url_blacklist(url):
        return True
    scheme, netloc, path, query, fragment = urlsplit(url)
    path: str
    parts = path.rsplit('.', 1)
    if len(parts) < 2:
        return False
    ext = parts[1].lower()
    return ext in _DOTWHAT_BLACKLIST


def _parse_opml(text):
    result = {}
    result['items'] = items = []
    raw = listparser.parse(io.StringIO(text))
    bozo_exception = raw.get('bozo_exception')
    if bozo_exception:
        LOG.warning(f'Parse OPML {bozo_exception}')
    result['title'] = (raw['meta'] or {}).get('title')
    for feed in (raw['feeds'] or []):
        url = feed.get('url')
        title = feed.get('title')
        # ignore title if it's url. eg: rssant before v1.8 export text(title) field with feed link
        if title and RE_URL.match(title):
            title = None
        # eg: {'url': '...', 'title': '...', 'categories': [['设计']], 'tags': ['设计']}
        categories = feed.get('categories')
        group = categories[0] if categories else None
        if group and isinstance(group, list):
            group = group[0]
        group = str(group) if group is not None else None
        if not url:
            continue
        url = _normalize_url(url)
        items.append(dict(
            title=title,
            group=group,
            url=url,
        ))
    total = len(result['items'])
    if total > IMPORT_ITEMS_LIMIT:
        LOG.warning(f'import {total} OPML feeds exceed limit {IMPORT_ITEMS_LIMIT}, will discard!')
        result['items'] = result['items'][:IMPORT_ITEMS_LIMIT]
    result = validate_opml(result)
    result['items'] = [x for x in result['items'] if x['url']]
    return result


def _normalize_url(url):
    """
    >>> _normalize_url('https://blog.guyskk.com/blog/1#title')
    'https://blog.guyskk.com/blog/1'
    >>> _normalize_url('HTTPS://t.cn/123ABC?q=1')
    'https://t.cn/123ABC?q=1'
    """
    return str(yarl.URL(url).with_fragment(None))


def _parse_text(text):
    """
    >>> _parse_text('https://www.example.com/aaa.bbb.JPG')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.JPEG')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.TTF')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.js')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.mp3')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.avi')
    []
    >>> _parse_text('https://www.example.com/aaa.bbb.tar.gz')
    []
    """
    tmp_urls = set()
    for match in RE_URL.finditer(text):
        url = match.group(0).strip()
        if not _is_in_blacklist(url):
            tmp_urls.add(url)
    urls = []
    for url in tmp_urls:
        try:
            url = _validate_url(url)
        except Invalid:
            pass  # ignore
        else:
            urls.append(_normalize_url(url))
    total = len(urls)
    if total > IMPORT_ITEMS_LIMIT:
        LOG.warning(f'import {total} feed urls exceed limit {IMPORT_ITEMS_LIMIT}, will discard!')
        urls = urls[:IMPORT_ITEMS_LIMIT]
    return urls


def _import_one_line_text(text):
    text = text.strip()
    parts = text.split(maxsplit=2)
    if len(parts) != 1:
        return None
    url = coerce_url(parts[0])
    try:
        _validate_url(url)
    except Invalid:
        return None
    return url


def import_feed_from_text(text, filename=None) -> typing.List[dict]:
    r"""
    >>> text = "<opml> https://blog.guyskk.com/blog/1 https://blog.anyant.com"
    >>> expect = set(['https://blog.guyskk.com/blog/1', 'https://blog.anyant.com'])
    >>> items = import_feed_from_text(text)
    >>> set(x['url'] for x in items) == expect
    True
    >>> items = import_feed_from_text(text, filename='aaa.txt')
    >>> set(x['url'] for x in items) == expect
    True
    >>> items = import_feed_from_text('blog.guyskk.com ')
    >>> [x['url'] for x in items]
    ['http://blog.guyskk.com']
    >>> text = '\n'.join(f'http://feed.com/{i}' for i in range(IMPORT_ITEMS_LIMIT + 10))
    >>> len(import_feed_from_text(text)) == IMPORT_ITEMS_LIMIT
    True
    >>> len(import_feed_from_text('<opml>\n' + text)) == IMPORT_ITEMS_LIMIT
    True
    """
    url = _import_one_line_text(text)
    if url is not None:
        return [dict(url=url)]
    if filename and _RE_OPML_FILENAME.match(filename):
        maybe_opml = True
    elif '<opml' in text[:1000] or '<?xml' in text[:1000]:
        maybe_opml = True
    else:
        maybe_opml = False
    result = {}
    if maybe_opml:
        LOG.info('import text maybe OPML/XML, try parse it by OPML/XML parser')
        try:
            opml_result = _parse_opml(text)
        except Invalid as ex:
            LOG.warning('parse opml failed, will fallback to general text parser', exc_info=ex)
        else:
            for item in opml_result['items']:
                result[item['url']] = item
    if not result:
        urls = _parse_text(text)
        for url in urls:
            result[url] = dict(url=url)
    return list(sorted(result.values(), key=lambda x: x['url']))
