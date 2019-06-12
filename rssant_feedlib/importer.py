import re
import logging
from collections import namedtuple
from urllib.parse import urlsplit, urlunsplit

from validr import Invalid
from xml.etree.ElementTree import ParseError

from .opml import parse_opml
from .bookmark import parse_bookmark

LOG = logging.getLogger(__name__)

FeedItem = namedtuple('FeedItem', 'url, title')
RE_OPML_FILENAME = re.compile(r'^.*\.(opml|xml)$', re.I)


def remove_url_fragment(url):
    """
    >>> remove_url_fragment('https://blog.guyskk.com/blog/1#title')
    'https://blog.guyskk.com/blog/1'
    """
    scheme, netloc, path, query, fragment = urlsplit(url)
    return urlunsplit((scheme, netloc, path, query, None))


def import_feed_from_text(text, filename=None) -> [str]:
    """
    >>> text = "<opml> https://blog.guyskk.com/blog/1 https://blog.anyant.com"
    >>> expect = set(['https://blog.guyskk.com/blog/1', 'https://blog.anyant.com'])
    >>> set(import_feed_from_text(text)) == expect
    True
    >>> set(import_feed_from_text(text, filename='aaa.txt')) == expect
    True
    """
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
        except (Invalid, ParseError) as ex:
            LOG.warning('parse opml failed, will fallback to general text parser', exc_info=ex)
        else:
            for item in opml_result['items']:
                result.add(remove_url_fragment(item['url']))
    if not result:
        urls = parse_bookmark(text)
        for url in urls:
            result.add(remove_url_fragment(url))
    return list(result)
