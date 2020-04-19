import re
import logging
from pathlib import Path
from collections import namedtuple
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree

from validr import T, Invalid

from rssant_common.helper import coerce_url
from rssant_common.validator import compiler
from .schema import validate_opml
from .blacklist import compile_url_blacklist
from .helper import lxml_call, LXMLError

LOG = logging.getLogger(__name__)

FeedItem = namedtuple('FeedItem', 'url, title')
RE_OPML_FILENAME = re.compile(r'^.*\.(opml|xml)$', re.I)
RE_URL = re.compile(
    r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"  # noqa
)
validate_url = compiler.compile(T.url)


BLACKLIST_CONTENT = """
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


is_in_url_blacklist = compile_url_blacklist(BLACKLIST_CONTENT)


def load_dotwhat_blacklist() -> set:
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


DOTWHAT_BLACKLIST = load_dotwhat_blacklist()


def is_in_blacklist(url: str):
    if is_in_url_blacklist(url):
        return True
    scheme, netloc, path, query, fragment = urlsplit(url)
    path: str
    parts = path.rsplit('.', 1)
    if len(parts) < 2:
        return False
    ext = parts[1].lower()
    return ext in DOTWHAT_BLACKLIST


def parse_opml(text):
    result = {}
    result['items'] = items = []
    root = lxml_call(ElementTree.fromstring, text)
    title = root.find('./head/title')
    if title is not None:
        title = title.text
    else:
        title = ''
    result['title'] = title
    for node in root.findall('./body//outline'):
        url = node.attrib.get('xmlUrl')
        if not url:
            continue
        items.append({
            'title': node.attrib.get('title'),
            'type': node.attrib.get('type'),
            'url': url,
        })
    result = validate_opml(result)
    return result


def remove_url_fragment(url):
    """
    >>> remove_url_fragment('https://blog.guyskk.com/blog/1#title')
    'https://blog.guyskk.com/blog/1'
    """
    scheme, netloc, path, query, fragment = urlsplit(url)
    return urlunsplit((scheme, netloc, path, query, None))


def parse_text(text):
    """
    >>> parse_text('https://www.example.com/aaa.bbb.JPG')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.JPEG')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.TTF')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.js')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.mp3')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.avi')
    []
    >>> parse_text('https://www.example.com/aaa.bbb.tar.gz')
    []
    """
    tmp_urls = set()
    for match in RE_URL.finditer(text):
        url = match.group(0).strip()
        if not is_in_blacklist(url):
            tmp_urls.add(url)
    urls = []
    for url in tmp_urls:
        try:
            url = validate_url(url)
        except Invalid:
            pass  # ignore
        else:
            urls.append(url)
    urls = list(sorted(urls))
    return urls


def import_one_line_text(text):
    text = text.strip()
    parts = text.split(maxsplit=2)
    if len(parts) != 1:
        return None
    url = coerce_url(parts[0])
    try:
        validate_url(url)
    except Invalid:
        return None
    return url


def import_feed_from_text(text, filename=None) -> [str]:
    """
    >>> text = "<opml> https://blog.guyskk.com/blog/1 https://blog.anyant.com"
    >>> expect = set(['https://blog.guyskk.com/blog/1', 'https://blog.anyant.com'])
    >>> set(import_feed_from_text(text)) == expect
    True
    >>> set(import_feed_from_text(text, filename='aaa.txt')) == expect
    True
    >>> import_feed_from_text('blog.guyskk.com ')
    ['http://blog.guyskk.com']
    """
    url = import_one_line_text(text)
    if url is not None:
        return [url]
    if filename and RE_OPML_FILENAME.match(filename):
        maybe_opml = True
    elif '<opml' in text[:1000] or '<?xml' in text[:1000]:
        maybe_opml = True
    else:
        maybe_opml = False
    result = set()
    if maybe_opml:
        LOG.info('import text maybe OPML/XML, try parse it by OPML/XML parser')
        try:
            opml_result = parse_opml(text)
        except (Invalid, LXMLError) as ex:
            LOG.warning('parse opml failed, will fallback to general text parser', exc_info=ex)
        else:
            for item in opml_result['items']:
                result.add(remove_url_fragment(item['url']))
    if not result:
        urls = parse_text(text)
        for url in urls:
            result.add(remove_url_fragment(url))
    return list(result)
