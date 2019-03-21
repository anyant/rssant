import logging
import cgi
import socket
import ssl
from urllib.parse import urlsplit, urlunsplit, unquote, urljoin

from bs4 import BeautifulSoup
import requests

from .parser import FeedParser
from .reader import FeedReader
from .helper import coerce_url

LOG = logging.getLogger(__name__)


def _to_list(d):
    return list(sorted(d.items(), key=lambda x: x[0], reverse=True))


# URL路径后缀：是Feed的可能性
URL_ENDS_FEED = _to_list(
    {
        "feed": 0.8,
        "atom.xml": 0.8,
        "rss.xml": 0.8,
        ".atom": 0.5,
        ".rss": 0.5,
        ".xml": 0.2,
        ".json": 0.1,
    }
)
# URL路径后缀，只要匹配即排除
URL_ENDS_NOT_FEED = {
    ".js",
    ".css",
    ".jpeg",
    ".jpg",
    ".png",
    ".gif",
    ".svg",
    "manifest.json",
    "opensearch.xml",
}

# URL路径，判断是否包含
URL_PATH_FEED = _to_list({'feed': 0.1, 'atom': 0.1, 'rss': 0.1})

# link rel属性，判断是否包含
LINK_REL_FEED = _to_list({"alternate": 0.9})
# link rel属性，只要包含即排除
LINK_REL_NOT_FEED = {
    "stylesheet",
    "dns-prefetch",
    "icon",
    "shortcut",
    "profile",
    "edituri",
    "pingback",
    "preload",
    "wlwmanifest",
    "bookmark",
    "author",
    "category",
    "tag",
    "nofollow",
}

# link type属性，判断是否包含
LINK_TYPE_FEED = _to_list(
    {
        "application/x.atom+xml": 0.9,
        "application/atom+xml": 0.9,
        "application/rss+xml": 0.9,
        "application/xml": 0.5,
        "text/xml": 0.5,
        "application/json": 0.5,
        "atom": 0.2,
        "rss": 0.2,
        "xml": 0.1,
        "json": 0.1,
    }
)
# link type属性，只要包含即排除
LINK_TYPE_NOT_FEED = {'text/css', "text/javascript"}


# HTTP响应Content-Type，只要包含即排除
CONTENT_TYPE_NOT_FEED = {
    'application/octet-stream',
    'application/javascript',
    'application/vnd.',
    'text/css',
    'text/csv',
    'image/',
    'font/',
    "audio/",
    'video/',
}

# 取HTTTP响应内容前200个字符，通过开头几个字符区分JSON和XML
# XML Feed格式，判断是否包含，分数累加
CONTENT_XML_FEED = _to_list(
    {
        '<rss': 0.5,
        '<atom': 0.5,
        '<feed': 0.3,
        '<link': 0.2,
        '<title': 0.2,
        '<author': 0.2,
        '<generator': 0.2,
        'version': 0.1,
    }
)

# HTML格式，判断是否包含，分数累加
CONTENT_HTML = _to_list(
    {
        '<!doctype html>': 1.0,
        '<html': 0.8,
        'html': 0.2,
        '<head': 0.2,
        '<meta': 0.2,
        '<title': 0.1,
    }
)

# JSON Feed格式，判断是否包含，分数累加
CONTENT_JSON_FEED = _to_list(
    {
        'version': 0.2,
        'title': 0.2,
        'author': 0.2,
        'description': 0.2,
        'feed': 0.3,
        'items': 0.3,
        'feed_url': 0.5,
    }
)

MAYBE_FEEDS = ["feed", "atom.xml", "feed.xml", "rss.xml", "rss", "index.xml"]


class ScoredLink:
    def __init__(self, url, score):
        self.url = url
        self.score = score

    def __repr__(self):
        return f"<Link {self.url} score={self.score:.3f}>"


class FeedFinder:
    """
    Usage:

        finder = FeedFinder(start_url)
        result = finder.find()
        finder.close()
    Args:
        start_url: start url
        message_handler: callable (str) -> None
    """

    def __init__(self, start_url, message_handler=None, max_trys=10, reader=None, validate=True):
        start_url = unquote(coerce_url(start_url))
        self._set_start_url(start_url)
        self.message_handler = message_handler
        self.max_trys = max_trys
        if reader is None:
            reader = FeedReader()
            self._close_reader = True
        else:
            self._close_reader = False
        self.reader = reader
        self.validate = validate
        self._links = {start_url: ScoredLink(start_url, 1.0)}
        self._visited = set()

    def _log(self, msg):
        if self.message_handler:
            self.message_handler(msg)
        else:
            LOG.debug(msg)

    def _set_start_url(self, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        if not scheme or not netloc:
            raise ValueError(f"invalid start_url {url!r}")
        self.start_url = url
        self.scheme = scheme
        self.netloc = netloc
        self.path = path

    def _read(self, url, current_try):
        self._visited.add(url)
        try:
            res = self.reader.read(url)
            if current_try == 0 and res.history:
                # 发生了重定向，重新设置start_url
                url = unquote(res.url)
                self._log(f'resolve redirect, set start url to {url}')
                self._set_start_url(url)
            return res
        except requests.exceptions.HTTPError as ex:
            self._log(str(ex))
        except (
            socket.gaierror,
            socket.timeout,
            TimeoutError,
            ConnectionError,
            ssl.SSLError,
            requests.exceptions.SSLError,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
        ) as ex:
            msg = type(ex).__name__ + ': ' + str(ex)
            self._log(f"{msg} when request {url!r}")
        except Exception as ex:
            msg = f"Error raised when request {url!r}"
            LOG.error(msg, exc_info=ex)
            self._log(msg)
        return None

    def _parse(self, response):
        content_type = response.headers.get('content-type', '').lower()
        mime_type, __ = cgi.parse_header(content_type)
        if mime_type:
            msg = f'Content-Type {mime_type} is considered not feed'
            for key in CONTENT_TYPE_NOT_FEED:
                if key in mime_type:
                    self._log(msg)
                    return None
        # 取前200个字符，快速判断
        head200 = response.text[:200].strip().lower()
        if 'json' in mime_type or head200.startswith('{'):
            return self._parse_feed(response)
        if '<!doctype html>' in head200[:50]:
            msg = "the response content is HTML, not XML feed"
            self._log(msg)
            self._parse_html(response)
            return None
        # 判断是HTML还是XMLFeed
        p_html = 0
        p_feed = 0
        if 'html' in mime_type:
            p_html += 0.5
        if 'xml' in mime_type:
            p_feed += 0.5
        for key, score in CONTENT_HTML:
            if key in head200:
                p_html += score
        for key, score in CONTENT_XML_FEED:
            if key in head200:
                p_feed += score
        if (p_html + 1) / (p_feed + 1) > 1.0:
            msg = "the response content is considered HTML, not XML feed"
            self._log(msg)
            self._parse_html(response)
            return None
        return self._parse_feed(response)

    def _parse_feed(self, response):
        result = FeedParser.parse_response(response, validate=self.validate)
        if not result.bozo:
            return result
        msg = f"{result.bozo_exception}, (...total {result.bozo} errors)"
        self._log(msg)

    def _parse_html(self, response):
        links = self._find_links(response.text, unquote(response.url))
        # 按得分从高到低排序，取前 max_trys 个
        links = list(sorted(links, key=lambda x: x.score, reverse=True))
        links = links[: self.max_trys]
        self._merge_links(links)

    def _merge_links(self, links):
        # 更新links，相同的合并
        for link in links:
            old = self._links.get(link.url, None)
            if old is not None:
                if link.score > old.score:
                    self._links[link.url] = link
            else:
                self._links[link.url] = link

    def _find_links(self, text, page_url):
        soup = BeautifulSoup(text, "html.parser")
        links = []
        for tag in soup.find_all(["link", "a"]):
            link = self._parse_link(tag, page_url)
            if link is None:
                continue
            if link.url in self._visited:
                continue
            links.append(link)
        return links

    def _parse_link(self, tag, page_url):
        link_rel = tag.get("rel", "")
        if not isinstance(link_rel, str):
            link_rel = ' '.join(link_rel)
        link_rel = link_rel.lower()
        if link_rel:
            for key in LINK_REL_NOT_FEED:
                if key in link_rel:
                    return None
        link_type = str(tag.get("type", "")).lower()
        if link_type:
            for key in LINK_TYPE_NOT_FEED:
                if key in link_type:
                    return None
        url = tag.get("href", "")
        if not url:
            return None
        if not (url.startswith('http://') or url.startswith('https://')):
            url = urljoin(page_url, url)  # 处理相对路径
        scheme, netloc, path, query, fragment = urlsplit(url)
        if (not netloc) or netloc != self.netloc:
            return None
        if not scheme:
            scheme = self.scheme
        else:
            scheme = scheme.lower()
            if scheme == 'feed':
                scheme = self.scheme
            elif scheme not in {'http', 'https'}:
                return None
        lower_path = path.lower()
        for key in URL_ENDS_NOT_FEED:
            if lower_path.endswith(key):
                return None
        url = urlunsplit((scheme, netloc, path, query, fragment))
        return self._score_link(url, lower_path, link_rel, link_type)

    def _score_link(self, url, path, link_rel, link_type):
        s = 0
        for key, score in URL_ENDS_FEED:
            if path.endswith(key):
                s += score
                break
        for key, score in URL_PATH_FEED:
            if key in path:
                s += score
                break
        if link_rel:
            for key, score in LINK_REL_FEED:
                if key in link_rel:
                    s += score
                    break
        if link_type:
            for key, score in LINK_TYPE_FEED:
                if key in link_type:
                    s += score
                    break
        s += 0.020 - len(path) * 0.001  # 分数相差不大时，越短的路径越好
        return ScoredLink(unquote(url), s)

    def _guess_links(self):
        path_segments = self.path.split("/")
        maybe_contains_feed = []
        maybe_feed = []
        root = urlunsplit((self.scheme, self.netloc, "", "", ""))
        maybe_contains_feed.append(ScoredLink(root, 0.5))
        for i in range(len(path_segments)):
            path = "/".join(path_segments[:i])
            url = urlunsplit((self.scheme, self.netloc, path, "", ""))
            maybe_contains_feed.append(ScoredLink(url, 1.0 / (i + 3)))
            for k in MAYBE_FEEDS:
                path = "/".join(path_segments[:i] + [k])
                url = urlunsplit((self.scheme, self.netloc, path, "", ""))
                maybe_feed.append(ScoredLink(url, 1.0 / (i + 4)))
        links = maybe_contains_feed + maybe_feed
        self._merge_links(links)

    def _pop_candidate(self):
        if not self._links:
            return None
        links = list(sorted(self._links.values(), key=lambda x: x.score))
        ret = links[-1].url
        del self._links[ret]
        if ret in self._visited:
            return self._pop_candidate()
        return ret

    def find(self):
        for current_try in range(self.max_trys):
            url = self._pop_candidate()
            if not url:
                self._log(f"No more candidate url")
                break
            self._log(f"#{current_try} try {url}")
            res = self._read(url, current_try)
            if res is None:
                if current_try == 0 and not self._links:
                    self._log(
                        f'{url} not reachable or not contain links, '
                        f'will guess some links from it'
                    )
                    self._guess_links()
                continue
            result = self._parse(res)
            if result is None:
                continue
            entries = result.entries
            version = result.version
            title = result.feed["title"]
            msg = f"Feed: version={version}, title={title}, has {len(entries)} entries"
            self._log(msg)
            return result
        self._log('Not found any valid feed!')
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self._close_reader:
            self.reader.close()


def _main():
    urls = [
        "ruanyifeng.com",
        "www.guyskk.com",
        "https://arp242.net/feed.xml",
        "https://www.imququ.com",
        "blog.guyskk.com",
        "http://www.zreading.cn/ican/2010/03/feed-subscribe/",
        "http://www.ruanyifeng.com/blog/",
        "https://www.zhihu.com",
        "https://www.zhihu.com/question/19580096",
    ]
    for url in urls:
        print("-" * 80)
        finder = FeedFinder(url)
        result = finder.find()
        if result:
            print(f"Got: " + str(result.feed)[:300] + "\n")
        finder.close()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
    )
    LOG.setLevel("DEBUG")
    feeds = []
    _main()
    # from pyinstrument import Profiler
    # profiler = Profiler()
    # profiler.start()
    # run(_main())
    # profiler.stop()
    # print(profiler.output_text(unicode=True, color=True))
