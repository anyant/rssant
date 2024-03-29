import logging
from typing import Tuple
from urllib.parse import urlsplit, urlunsplit, unquote, urljoin

from bs4 import BeautifulSoup
from validr import Invalid

from rssant_common.helper import coerce_url
from rssant_common.dns_service import DNS_SERVICE

from .raw_parser import RawFeedParser, FeedParserError, RawFeedResult
from .reader import FeedReader
from .response import FeedResponse, FeedResponseStatus
from .processor import validate_url


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
    ".bmp",
    ".zip",
    ".gz",
    ".tgz",
    ".rar",
    ".jar",
    ".iso",
    "manifest.json",
    "opensearch.xml",
}

# URL路径，判断是否包含
URL_PATH_FEED = _to_list({'feed': 0.1, 'atom': 0.1, 'rss': 0.1})

# URL包含评论字样，降低权重
URL_COMMENT = _to_list({
    'comment': 0.1,
    '评论': 0.1,
    '評論': 0.1,
})

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

    def __init__(
        self,
        start_url,
        message_handler=None,
        max_trys=10,
        reader=None,
        request_timeout=30,
        proxy_url=None,
        rss_proxy_url=None,
        rss_proxy_token=None,
        dns_service=DNS_SERVICE,
    ):
        start_url = coerce_url(start_url)
        try:
            start_url = validate_url(start_url)
        except Invalid:
            raise ValueError(f"invalid start_url {start_url!r}")
        self._set_start_url(start_url)
        self.message_handler = message_handler
        self.max_trys = max_trys
        if reader is None:
            reader = FeedReader(
                dns_service=dns_service,
                proxy_url=proxy_url,
                rss_proxy_url=rss_proxy_url,
                rss_proxy_token=rss_proxy_token,
                request_timeout=request_timeout,
            )
            self._close_reader = True
        else:
            self._close_reader = False
        self.reader = reader
        self._links = {start_url: ScoredLink(start_url, 1.0)}
        self._visited = set()
        self._guessed = False

    @property
    def has_proxy(self):
        return self.reader.has_proxy

    def _log(self, msg):
        if self.message_handler:
            self.message_handler(msg)
        else:
            LOG.debug(msg)

    def _set_start_url(self, url):
        scheme, netloc, path, query, fragment = urlsplit(url)
        assert scheme and netloc, f"invalid start_url {url!r}"
        self.start_url = url
        self.scheme = scheme
        self.netloc = netloc
        self.path = path

    def _read(self, url, current_try, use_proxy=False):
        self._visited.add(url)
        res = self.reader.read(url, use_proxy=use_proxy)
        if res.ok and current_try == 0 and res.url != url:
            # 发生了重定向，重新设置start_url
            try:
                url = validate_url(res.url)
            except Invalid:
                self._log(f'invalid redirect url {unquote(res.url)}')
            else:
                self._log(f'resolve redirect, set start url to {unquote(url)}')
                self._set_start_url(url)
        if not res.ok:
            error_name = FeedResponseStatus.name_of(res.status)
            msg = '{} {} when request {!r}'.format(res.status, error_name, url)
            self._log(msg)
        return res

    def _parse(self, response: FeedResponse) -> RawFeedResult:
        if response.feed_type.is_html:
            msg = "the response content is HTML, not XML feed"
            self._log(msg)
            self._parse_html(response)
            return None
        if response.feed_type.is_other:
            msg = "the response content is not any feed type"
            self._log(msg)
            return None
        raw_parser = RawFeedParser()
        try:
            result = raw_parser.parse(response)
        except FeedParserError as ex:
            self._log(str(ex))
            return None
        if result.warnings:
            msg = f"warnings: {';'.join(result.warnings)}"
            self._log(msg)
            LOG.warning(msg)
        return result

    def _parse_html(self, response):
        text = response.content.decode(response.encoding, errors='ignore')
        links = self._find_links(text, response.url)
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
        try:
            scheme, netloc, path, query, fragment = urlsplit(url)
        except ValueError as ex:
            # eg: netloc '210.41.222.154：8088' contains invalid characters
            LOG.warning('parse link %r failed: %s', url, ex)
            return None
        base_netloc = '.'.join(netloc.rsplit('.', 2)[-2:])
        if (not netloc) or base_netloc not in self.netloc:
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
        url = urlunsplit((scheme, netloc, path, query, None))
        try:
            validate_url(url)
        except Invalid:
            return None
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
        for key, score in URL_COMMENT:
            if key in url.lower():
                s -= score
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
        if url.startswith('https://'):
            s += 0.001  # 分数相差不大时，https更好
        s += 0.020 - len(path) * 0.001  # 分数相差不大时，越短的路径越好
        return ScoredLink(url, s)

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

    def _try_guess_links(self):
        if not self._links and not self._guessed:
            msg = 'guess some links from start_url'
            self._log(msg)
            self._guess_links()
            self._guessed = True

    def find(self, use_proxy=False) -> Tuple[FeedResponse, RawFeedResult]:
        current_try = 0
        should_abort = FeedResponseStatus.is_permanent_failure
        while current_try < self.max_trys:
            current_try += 1
            url = self._pop_candidate()
            if not url:
                self._log("No more candidate url")
                break
            self._log(f"#{current_try} try {url}")
            res = self._read(url, current_try, use_proxy=use_proxy)
            if self.has_proxy and not use_proxy:
                if FeedResponseStatus.is_need_proxy(res.status):
                    current_try += 1
                    self._log(f'#{current_try} try use proxy')
                    res = self._read(url, current_try, use_proxy=True)
                    if not should_abort(res.status):
                        use_proxy = True
            if should_abort(res.status):
                self._log('The url is unable to connect or likely not contain feed, abort!')
                break
            if not res.ok or not res.content:
                self._try_guess_links()
                continue
            result = self._parse(res)
            if result is None:
                self._try_guess_links()
                continue
            num_storys = len(result.storys)
            version = result.feed['version']
            title = result.feed["title"]
            msg = f"Feed: version={version}, title={title}, has {num_storys} storys"
            self._log(msg)
            return res, result
        self._log('Not found any feed!')
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        if self._close_reader:
            self.reader.close()
