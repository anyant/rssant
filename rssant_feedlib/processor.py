import typing
import re
from collections import namedtuple
from urllib.parse import urljoin, quote
import lxml.etree
import lxml.html
from lxml.html import soupparser
from lxml.html.defs import safe_attrs as lxml_safe_attrs
from lxml.html.clean import Cleaner
from readability import Document as ReadabilityDocument

from .importer import RE_URL
from .helper import lxml_call

RE_IMG = re.compile(
    r'(?:<img\s*[^<>]*?\s+src="([^"]+?)")|'
    r'(?:<source\s*[^<>]*?\s+srcset="([^"]+?)")',
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
    r'(\$[^\$]+?\$)|'                           # $...$
    r'(\`[^\`]+?\`)'                             # `...`
), re.I)


def story_has_mathjax(content):
    r"""
    >>> story_has_mathjax(r'2.7.5/MathJax.js?config=TeX-MML-AM_CHTML')
    True
    >>> story_has_mathjax(r'hi $$x^2$$ ok?')
    True
    >>> story_has_mathjax(r'hi \(x^2\), ok?')
    True
    >>> story_has_mathjax(r'hi \[x^2\], ok?')
    True
    >>> story_has_mathjax(r'hi $$x^2$$ ok?')
    True
    >>> story_has_mathjax(r'hi $x^2$ ok?')
    True
    >>> story_has_mathjax(r'hi `x^2` ok?')
    True
    """
    if not content:
        return False
    return bool(RE_MATHJAX.search(content))


StoryImageIndexItem = namedtuple('StoryImageIndexItem', 'pos, endpos, value')


def is_data_url(url):
    return url and url.startswith('data:')


RSSANT_IMAGE_TAG = 'rssant=1'


def is_replaced_image(url):
    """
    >>> is_replaced_image('https://rss.anyant.com/123.jpg?rssant=1')
    True
    """
    return url and RSSANT_IMAGE_TAG in url


def make_absolute_url(url, base_href):
    if not base_href:
        return url
    if not url.startswith('http://') and not url.startswith('https://'):
        url = urljoin(base_href, url)
    return url


class StoryImageProcessor:
    """
    >>> content = '''
    ... <picture class="kg-image lightness-target">
    ...     <source srcset="/abc.webp" type="image/webp">
    ...     <source
    ...     srcset="/abc.jpg
    ...         " type="image/jpeg">
    ...     <img src="/abc.jpg" alt="Design System实践"><img src="https://image.example.com/2019/12/21/xxx.jpg" alt="xxx image">
    ...     <img src="data:text/plain;base64,SGVsbG8sIFdvcmxkIQ%3D%3D" alt="DataURL">
    ... </picture>
    ... <img data-src="/error.jpg" src="/ok.jpg">
    ... '''
    >>> story_image_count(content)
    6
    >>> processor = StoryImageProcessor("https://rss.anyant.com/story/123", content)
    >>> image_indexs = processor.parse()
    >>> len(image_indexs)
    5
    >>> image_indexs[0].value
    'https://rss.anyant.com/abc.webp'
    >>> image_indexs[1].value
    'https://rss.anyant.com/abc.jpg'
    >>> image_indexs[2].value
    'https://rss.anyant.com/abc.jpg'
    >>> image_indexs[3].value
    'https://image.example.com/2019/12/21/xxx.jpg'
    >>> image_indexs[4].value
    'https://rss.anyant.com/ok.jpg'
    """  # noqa: E501

    def __init__(self, story_url, content):
        self.story_url = story_url
        self.content = content

    def fix_relative_url(self, url):
        return make_absolute_url(url, self.story_url)

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
            if not is_data_url(img_url) and not is_replaced_image(img_url):
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


IMG_EXT_SRC_ATTRS = ['data-src', 'data-original', 'data-origin']
RE_IMAGE_URL = re.compile(
    '(img|image|pic|picture|photo|png|jpg|jpeg|webp|bpg|ico|exif|tiff|gif|svg|bmp)', re.I)


def is_image_url(url):
    if not url:
        return False
    if is_data_url(url):
        return False
    return bool(RE_IMAGE_URL.search(url))


def process_story_links(content, story_link):
    """
    NOTE: Don't process_story_links after StoryImageProcessor, the replaced
        image urls will broken.
    >>> x = '<a href="/story/123.html">汉字</a>'
    >>> result = process_story_links(x, 'http://blog.example.com/index.html')
    >>> expect = '<a href="http://blog.example.com/story/123.html" target="_blank" rel="nofollow">汉字</a>'
    >>> assert list(sorted(result)) == list(sorted(expect)), result
    >>> x = '<img data-src="/story/123.png">'
    >>> result = process_story_links(x, 'http://blog.example.com/index.html')
    >>> expect = '<img data-src="/story/123.png" src="http://blog.example.com/story/123.png">'
    >>> assert list(sorted(result)) == list(sorted(expect)), result
    """
    if not content:
        return content
    dom = lxml_call(lxml.html.fromstring, content)
    for a in dom.iter('a'):
        url = a.get('href')
        if url:
            a.set('href', make_absolute_url(url, story_link))
        a.set('target', '_blank')
        a.set('rel', 'nofollow')
    for x in dom.iter('img'):
        ext_src = None
        for key in IMG_EXT_SRC_ATTRS:
            value = x.get(key)
            if is_image_url(value):
                ext_src = value
                break
        if ext_src:
            src = make_absolute_url(ext_src, story_link)
            x.set('src', src)
    # also make image, video... other links absolute
    if story_link:
        dom.make_links_absolute(story_link)
    result = lxml.html.tostring(dom, encoding='unicode')
    if isinstance(result, bytes):
        result = result.decode('utf-8')
    return result


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
    >>> print(story_html_to_text('<pre><code>hi</code></pre>'))
    <BLANKLINE>
    >>> content = '''
    ... <?xml version="1.0" encoding="utf-8"?>
    ... <?xml-stylesheet type="text/xsl" href="/res/preview.xsl"?>
    ... <p>中文传媒精选</p>
    ... '''
    >>> print(story_html_to_text(content))
    中文传媒精选
    """
    if (not content) or (not content.strip()):
        return ""
    try:
        if clean:
            # https://bugs.launchpad.net/lxml/+bug/1851029
            # The html cleaner raise AssertionError when both
            # root tag and child tag in kill_tags set.
            if content.startswith('<pre'):
                content = '<div>' + content + '</div>'
            content = lxml_call(lxml_text_html_cleaner.clean_html, content).strip()
        if not content:
            return ""
        r = lxml_call(lxml.html.fromstring, content, parser=lxml_html_parser)
        content = r.text_content().strip()
    except lxml.etree.ParserError:
        content = lxml_call(soupparser.fromstring, content).text_content().strip()
    return RE_BLANK_LINE.sub('\n', content)


RSSANT_HTML_SAFE_ATTRS = set(lxml_safe_attrs) | set(IMG_EXT_SRC_ATTRS)
RSSANT_HTML_SAFE_ATTRS.update({'srcset'})

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
    safe_attrs=RSSANT_HTML_SAFE_ATTRS,
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
    >>> content = '''
    ... <?xml version="1.0" encoding="utf-8"?>
    ... <?xml-stylesheet type="text/xsl" href="/res/preview.xsl"?>
    ... <p>中文传媒精选</p>
    ... '''
    >>> print(story_html_clean(content))
    <p>中文传媒精选</p>
    """
    if (not content) or (not content.strip()):
        return ""
    content = lxml_call(lxml_story_html_cleaner.clean_html, content).strip()
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
