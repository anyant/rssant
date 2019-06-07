import re
from collections import namedtuple
from urllib.parse import urljoin
from html2text import HTML2Text

RE_IMG = re.compile(r'<img\s*.*?\s*src="([^"]+?)"', re.I | re.M)

StoryImageIndexItem = namedtuple('StoryImageIndexItem', 'pos, endpos, value')


class StoryImageProcessor:
    def __init__(self, story_url, content):
        self.story_url = story_url
        self.content = content

    def fix_relative_url(self, url):
        if not url.startswith('http://') and not url.startswith('https://'):
            url = urljoin(self.story_url, url)
        return url

    def parse(self) -> [StoryImageIndexItem]:
        if not self.content:
            return
        content = self.content
        image_indexs = []
        pos = 0
        while True:
            match = RE_IMG.search(content, pos=pos)
            if not match:
                break
            img_url = self.fix_relative_url(match.group(1).strip())
            idx = StoryImageIndexItem(*match.span(1), img_url)
            image_indexs.append(idx)
            pos = match.end(1)
        return image_indexs

    def process(self, image_indexs, images) -> str:
        new_image_indexs = []
        for idx in image_indexs:
            new_url = images.get(idx.value)
            if new_url:
                idx = StoryImageIndexItem(idx.pos, idx.endpos, new_url)
            new_image_indexs.append(idx)
        content = self.content
        content_chunks = []
        beginpos = 0
        for pos, endpos, value in new_image_indexs:
            content_chunks.append(content[beginpos: pos])
            content_chunks.append(value)
            beginpos = endpos
        content_chunks.append(content[beginpos:])
        return ''.join(content_chunks)


def story_html_to_text(content):
    h = HTML2Text()
    h.ignore_links = True
    return h.handle(content or "")


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
