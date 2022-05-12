import re
from rssant_config import CONFIG
from rssant_common.blacklist import compile_url_blacklist


RE_V2EX = re.compile(r'^http(s)?://[a-zA-Z0-9_\.\-]*\.v2ex\.com', re.I)
RE_HACKNEWS = re.compile(r'^http(s)?://news\.ycombinator\.com', re.I)
RE_GITHUB = re.compile(r'^http(s)?://github\.com', re.I)
RE_PYPI = re.compile(r'^http(s)?://[a-zA-Z0-9_\.\-]*\.?pypi\.org', re.I)


def is_v2ex(url):
    """
    >>> is_v2ex("https://www.v2ex.com/t/466888#reply0")
    True
    >>> is_v2ex("http://www.v2ex.com/t/466888#reply0")
    True
    >>> is_v2ex("http://xxx.cdn.v2ex.com/image/test.png")
    True
    >>> is_v2ex("https://www.v2ex.net/t/466888#reply0")
    False
    """
    return bool(RE_V2EX.match(url))


def is_hacknews(url):
    """
    >>> is_hacknews("https://news.ycombinator.com/rss")
    True
    >>> is_hacknews("http://news.ycombinator.com/rss")
    True
    >>> is_hacknews("https://news.ycombinator.com/")
    True
    >>> is_hacknews("https://xxx.ycombinator.com/")
    False
    """
    return bool(RE_HACKNEWS.match(url))


def is_github(url):
    """
    >>> is_github("https://github.com/guyskk/rssant")
    True
    >>> is_github("http://github.com/guyskk")
    True
    >>> is_github("https://github.com")
    True
    >>> is_github("https://www.github.com/guyskk/rssant")
    False
    >>> is_github("http://guyskk.github.io/blog/xxx")
    False
    """
    return bool(RE_GITHUB.match(url))


def is_pypi(url):
    """
    >>> is_pypi("https://pypi.org/project/import-watch/1.0.0/")
    True
    >>> is_pypi("http://pypi.org")
    True
    >>> is_pypi("https://simple.pypi.org/index")
    True
    >>> is_pypi("https://pypi.python.org/index")
    False
    """
    return bool(RE_PYPI.match(url))


def is_rssant_changelog(url: str):
    """
    >>> is_rssant_changelog('http://localhost:6789/changelog?version=1.0.0')
    True
    >>> is_rssant_changelog('https://rss.anyant.com/changelog.atom')
    True
    >>> is_rssant_changelog('https://rss.anyant.xyz/changelog.atom')
    True
    >>> is_rssant_changelog('https://rss.qa.anyant.com/changelog.atom')
    True
    >>> is_rssant_changelog('https://www.anyant.com/')
    False
    """
    is_rssant = 'rss' in url and 'anyant' in url
    is_local_rssant = url.startswith(CONFIG.root_url)
    return (is_rssant or is_local_rssant) and 'changelog' in url


NOT_FETCH_FULLTEXT_LIST = '''
taoshu.in
cnki.net
'''

_is_in_not_fetch_list = compile_url_blacklist(NOT_FETCH_FULLTEXT_LIST)


def is_not_fetch_fulltext(url: str):
    """
    是否是不抓取原文的订阅链接

    >>> is_not_fetch_fulltext("https://www.v2ex.com/t/466888#reply0")
    True
    >>> is_not_fetch_fulltext('https://rss.anyant.com/changelog.atom')
    True
    >>> is_not_fetch_fulltext('https://taoshu.in/feed.xml')
    True
    >>> is_not_fetch_fulltext('https://blog.guyskk.com/feed.xml')
    False
    >>> is_not_fetch_fulltext('https://t.cnki.net/kcms/detail')
    True
    """
    checkers = [
        is_v2ex,
        is_hacknews,
        is_github,
        is_pypi,
        is_rssant_changelog,
        _is_in_not_fetch_list,
    ]
    for check in checkers:
        if check(url):
            return True
    return False
