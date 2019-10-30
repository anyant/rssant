import typing
import re
from collections import namedtuple
from urllib.parse import urljoin, quote
import lxml.etree
import lxml.html
from lxml.html import soupparser
from lxml.html.clean import Cleaner
from readability import Document as ReadabilityDocument

from .importer import RE_URL

RE_IMG = re.compile(
    r'(?:<img\s*.*?\s+src="([^"]+?)")|'
    r'(?:<source\s*.*?\s+srcset="([^"]+?)")',
    re.I | re.M)

RE_LINK = re.compile(r'<a\s*.*?\s+href="([^"]+?)"', re.I | re.M)


def story_image_count(content):
    if not content:
        return 0
    return len(RE_IMG.findall(content))


def story_url_count(content):
    """
    >>> content = '''
    ... <p><a class="xxx" href="https://rss.anyant.com/1">link1</a>
    ... http://www.example.com
    ... baidu.com asp.net
    ... <a href="https://rss.anyant.com/2" target="_blank">link2</a>
    ... </p>
    ... '''
    >>> story_url_count(content)
    3
    """
    if not content:
        return 0
    return len(RE_URL.findall(content))


def story_link_count(content):
    """
    >>> content = '''
    ... <p><a class="xxx" href="https://rss.anyant.com/1">link1</a>
    ... <a href="https://rss.anyant.com/2" target="_blank">link2</a>
    ... </p>
    ... '''
    >>> story_link_count(content)
    2
    """
    if not content:
        return 0
    return len(RE_LINK.findall(content))


RE_MATHJAX = re.compile((
    r'(MathJax)|(AsciiMath)|(MathML)|'          # keywords
    r'(\$\$[^\$]+?\$\$)|'                       # $$...$$
    r'(\\\([^\(\)]+?\\\))|'                     # \(...\)
    r'(\\\[[^\[\]]+?\\\])|'                     # \[...\]
    r'(<(code|pre)>\$[^\$]+?\$</(code|pre)>)|'  # <code>$...$</code> or <pre>$...$</pre>
    r'(<(code|pre)>\`[^\`]+\`</(code|pre)>)'    # <code>`...`</code> or <pre>`...`</pre>
), re.I)


def story_has_mathjax(content):
    r"""
    >>> story_has_mathjax('2.7.5/MathJax.js?config=TeX-MML-AM_CHTML')
    True
    >>> story_has_mathjax('hi $$x^2$$ ok?')
    True
    >>> story_has_mathjax('hi \(x^2\), ok?')
    True
    >>> story_has_mathjax('hi \[x^2\], ok?')
    True
    >>> story_has_mathjax('hi $$x^2$$ ok?')
    True
    >>> story_has_mathjax('hi <code>$x^2$</code> ok?')
    True
    >>> story_has_mathjax('hi <pre>$x^2$</pre> ok?')
    True
    >>> story_has_mathjax('hi <code>`x^2`</code> ok?')
    True
    >>> story_has_mathjax('hi <pre>`x^2`</pre> ok?')
    True
    """
    if not content:
        return False
    return bool(RE_MATHJAX.search(content))


StoryImageIndexItem = namedtuple('StoryImageIndexItem', 'pos, endpos, value')


class StoryImageProcessor:
    """
    >>> content = '''
    ... <picture class="kg-image lightness-target">
    ...     <source srcset="/abc.webp" type="image/webp">
    ...     <source
    ...     srcset="/abc.jpg
    ...         " type="image/jpeg">
    ...     <img src="/abc.jpg" alt="Design System实践">
    ...     <img src="data:text/plain;base64,SGVsbG8sIFdvcmxkIQ%3D%3D" alt="DataURL">
    ... </picture>
    ... <img data-src="/error.jpg" src="/ok.jpg">
    ... '''
    >>> story_image_count(content)
    5
    >>> processor = StoryImageProcessor("https://rss.anyant.com/story/123", content)
    >>> image_indexs = processor.parse()
    >>> len(image_indexs)
    4
    >>> image_indexs[0].value
    'https://rss.anyant.com/abc.webp'
    >>> image_indexs[1].value
    'https://rss.anyant.com/abc.jpg'
    >>> image_indexs[2].value
    'https://rss.anyant.com/abc.jpg'
    >>> image_indexs[3].value
    'https://rss.anyant.com/ok.jpg'
    """

    def __init__(self, story_url, content):
        self.story_url = story_url
        self.content = content

    def fix_relative_url(self, url):
        if not url.startswith('http://') and not url.startswith('https://'):
            url = urljoin(self.story_url, url)
        return url

    def is_data_url(self, url):
        return url.startswith('data:')

    def parse(self) -> typing.List[StoryImageIndexItem]:
        if not self.content:
            return []
        content = self.content
        image_indexs = []
        pos = 0
        while True:
            match = RE_IMG.search(content, pos=pos)
            if not match:
                break
            img_src, source_srcset = match.groups()
            startpos, endpos = match.span(1) if img_src else match.span(2)
            img_url = (img_src or source_srcset).strip()
            if not self.is_data_url(img_url):
                img_url = self.fix_relative_url(img_url)
                idx = StoryImageIndexItem(startpos, endpos, img_url)
                image_indexs.append(idx)
            pos = endpos
        return image_indexs

    def process(self, image_indexs, images) -> str:
        images = {quote(k): v for k, v in images.items()}
        new_image_indexs = []
        for idx in image_indexs:
            new_url = images.get(quote(idx.value))
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


def story_readability(content):
    """
    >>> content = '<p>hello world</p>'
    >>> print(story_readability(content))
    <body id="readabilityBody"><p>hello world</p></body>
    """
    if (not content) or (not content.strip()):
        return ""
    doc = ReadabilityDocument(content)
    return doc.summary(html_partial=True) or ""


RE_BLANK_LINE = re.compile(r'(\n\s*)(\n\s*)+')

lxml_html_parser = lxml.html.HTMLParser(
    remove_blank_text=True, remove_comments=True, collect_ids=False)

lxml_text_html_cleaner = Cleaner(
    scripts=True,
    javascript=True,
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=True,
    processing_instructions=True,
    embedded=True,
    frames=True,
    forms=True,
    annoying_tags=True,
    safe_attrs_only=True,
    add_nofollow=True,
    remove_tags=set(['body']),
    kill_tags=set(['code', 'pre', 'img', 'video', 'noscript']),
)


def story_html_to_text(content, clean=True):
    """
    >>> content = '''<html><body>
    ... <pre>hello world</pre>
    ...
    ...
    ... <p>happy day</p>
    ... </body></html>
    ... '''
    >>> print(story_html_to_text(content))
    happy day
    >>> print(story_html_to_text(content, clean=False))
    hello world
    happy day
    >>> content = '<![CDATA[hello world]]>'
    >>> print(story_html_to_text(content))
    hello world
    """
    if (not content) or (not content.strip()):
        return ""
    try:
        if clean:
            content = lxml_text_html_cleaner.clean_html(content).strip()
        if not content:
            return ""
        r = lxml.html.fromstring(content, parser=lxml_html_parser)
        content = r.text_content().strip()
    except lxml.etree.ParserError:
        content = soupparser.fromstring(content).text_content().strip()
    return RE_BLANK_LINE.sub('\n', content)


lxml_story_html_cleaner = Cleaner(
    scripts=True,
    javascript=True,
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=True,
    processing_instructions=True,
    embedded=True,
    frames=True,
    forms=True,
    annoying_tags=True,
    safe_attrs_only=True,
    add_nofollow=True,
    remove_tags=set(['body']),
    kill_tags=set(['noscript']),
)


def story_html_clean(content):
    """
    >>> content = '''<html><head><style></style></head><body>
    ... <pre stype="xxx">
    ...
    ... hello world</pre>
    ... <p>happy day</p>
    ... </body></html>
    ... '''
    >>> print(story_html_clean(content))
    <div>
    <pre>
    <BLANKLINE>
    hello world</pre>
    <p>happy day</p>
    </div>
    """
    if (not content) or (not content.strip()):
        return ""
    content = lxml_story_html_cleaner.clean_html(content).strip()
    if not content:
        return ""
    return content


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
