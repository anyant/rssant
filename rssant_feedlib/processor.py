import logging
import re
import typing
from collections import namedtuple
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

import lxml.etree
import lxml.html
import readability.cleaners
import yarl
from django.utils.html import escape as html_escape
from lxml.html import soupparser
from lxml.html.clean import Cleaner
from lxml.html.defs import safe_attrs as lxml_safe_attrs
from readability import Document as ReadabilityDocument
from validr import Invalid, T

from rssant_common.validator import compiler

from .helper import RE_URL, LXMLError, lxml_call

LOG = logging.getLogger(__name__)


def _patch_readability_cleaner():
    """
    image width and height is important for emoji or small icons,
    but python-readability will remove them.
    """
    # patch bad_attrs
    bad_attrs = readability.cleaners.bad_attrs
    for attr in ["width", "height"]:
        try:
            bad_attrs.remove(attr)
        except ValueError:
            pass  # ignore
    # patch htmlstrip
    htmlstrip_pattern = readability.cleaners.htmlstrip.pattern
    for attr in ["width", "height"]:
        htmlstrip_pattern = htmlstrip_pattern.replace(attr, '')
    htmlstrip = re.compile(htmlstrip_pattern, re.I)
    readability.cleaners.htmlstrip = htmlstrip


_patch_readability_cleaner()


validate_url = compiler.compile(T.url)


def _quote_path(path: str) -> str:
    """urllib.parse.quote会转义冒号等字符，这是不对的"""
    if not path:
        return ''
    return yarl.URL('https://rss.anyant.com/').with_path(path).raw_path


RE_IMG = re.compile(
    r'(?:<img\s*[^<>]*?\s+src="([^"]+?)")|'
    r'(?:<source\s*[^<>]*?\s+srcset="([^"]+?)")',
    re.I | re.M,
)

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


# implement by regex negative lookahead and negative lookbehind
# see also: https://regexr.com/
# $...$ but not $10...$10, 10$...10$ and jQuery $
_RE_MATHJAX_DOLLAR = r'(?<![^\s>])\$[^$\n]+?\$(?![^\s<])'
# `...` but not ```...```
_RE_MATHJAX_ASCIIMATH = r'(?<![^\s>])\`[^`\n]+?\`(?![^\s<])'

# loose regex for check MathJax
RE_MATHJAX = re.compile(
    (
        r'(\$\$.+?\$\$)|'  # $$...$$
        r'(\\\[.+?\\\])|'  # \[...\]
        r'(\\\(.+?\\\))|'  # \(...\)
        fr'({_RE_MATHJAX_DOLLAR})|'  # $...$
        fr'({_RE_MATHJAX_ASCIIMATH})'  # `...`
    ),
    re.I | re.M,
)


def story_has_mathjax(content):
    r"""
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
    >>> story_has_mathjax(r'hi $10 invest $10 ok?')
    False
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
    在v1.8之前，后端会检测图片是否需要代理，然后替换图片链接。
    之后改为了前端动态代理，去掉了后端处理步骤。这里的逻辑是为了兼容历史数据。

    >>> is_replaced_image('https://rss.anyant.com/123.jpg?rssant=1')
    True
    """
    return url and RSSANT_IMAGE_TAG in url


def _is_url(url):
    return bool(re.match(r'^https?:\/\/', url))


def make_absolute_url(url, base_href):
    if not base_href:
        return url
    if not _is_url(url):
        url = urljoin(base_href, url)
    return url


TOP_DOMAINS = set(
    """
com
org
net
edu
gov
tk
de
uk
cn
info
ru
nl
im
me
io
tech
top
xyz
""".strip().split()
)

RE_STICK_DOMAIN = re.compile(r'^({})[^\:\/\.$]+'.format('|'.join(TOP_DOMAINS)))


def normalize_url(url: str, base_url: str = None):
    """
    Normalize URL

    Note: not support urn and magnet
        urn:kill-the-newsletter:2wqcdaqwddn9lny1ewzy
        magnet:?xt=urn:btih:28774CFFE3B4715054E192FF
    """
    url = (url or '').strip()
    if not url:
        return url
    url = url.replace('：//', '://')
    url = url.replace('%3A//', '://')
    if url.startswith('://'):
        url = 'http' + url
    if not _is_url(url):
        # ignore urn: or magnet:
        if re.match(r'^[a-zA-Z0-9]+:', url):
            return url
        if base_url:
            url = urljoin(base_url, url)
        else:
            # ignore simple texts
            if not re.match(r'^(\.|\:|\/)?[a-zA-Z0-9\/]+(\.|\:|\/)', url):
                return url
            url = 'http://' + url
    # fix: http://www.example.comhttp://www.example.com/hello
    if url.count('://') >= 2:
        matchs = list(re.finditer(r'https?://', url))
        if matchs:
            url = url[matchs[-1].start(0) :]
        else:
            url = 'http://' + url.split('://')[-1]
    match = re.search(r'\.[^.]+?(\/|$)', url)
    if match:
        # fix: http://example.com%5Cblog
        match_text = unquote(match.group(0))
        match_text = match_text.replace('\\', '/')
        # fix: .comxxx -> .com/xxx
        stick_match = RE_STICK_DOMAIN.match(match_text[1:])
        # check match length to avoid break uncommon domain, eg: .dev
        if stick_match and len(stick_match.group(0)) >= 5:
            top_domain = stick_match.group(1)
            pre_len = 1 + len(top_domain)
            match_text = match_text[:pre_len] + '/' + match_text[pre_len:]
        url = url[: match.start()] + match_text + url[match.end() :]
    try:
        scheme, netloc, path, query, fragment = urlsplit(url)
    except ValueError as ex:
        # fix: http://example%5B.]com/x.php?age=23
        LOG.info(f'normalize failed: {ex} url={url!r}', exc_info=ex)
        return url
    # remove needless port
    if scheme == 'http' and netloc.endswith(':80'):
        netloc = netloc.rsplit(':', 1)[0]
    if scheme == 'https' and netloc.endswith(':443'):
        netloc = netloc.rsplit(':', 1)[0]
    # fix: http://example.com//blog
    path = re.sub(r'^\/\/+', '/', path)
    # quote is not idempotent, can not quote multiple times
    path = _quote_path(unquote(path))
    url = urlunsplit((scheme, netloc, path, query, fragment))
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
    ...     <img src="http://file///invalid.png">
    ...     <img src="data:text/plain;base64,SGVsbG8sIFdvcmxkIQ%3D%3D" alt="DataURL">
    ... </picture>
    ... <img data-src="/error.jpg" src="/ok.jpg">
    ... '''
    >>> story_image_count(content)
    7
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
                try:
                    validate_url(img_url)
                except Invalid:
                    pass
                else:
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
            content_chunks.append(content[beginpos:pos])
            content_chunks.append(value)
            beginpos = endpos
        content_chunks.append(content[beginpos:])
        return ''.join(content_chunks)


IMG_EXT_SRC_ATTRS = ['data-src', 'data-original', 'data-origin', 'data-options']
RE_IMAGE_URL = re.compile(
    '(img|image|pic|picture|photo|png|jpg|jpeg|webp|bpg|ico|exif|tiff|gif|svg|bmp)',
    re.I,
)


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
    >>> content = '<p>hello <b>world</b><br>你好<i>世界</i></p>'
    >>> print(story_readability(content))
    <body id="readabilityBody"><p>hello <b>world</b><br>你好<i>世界</i></p></body>
    >>> content = '<svg height="16" width="16" class="octicon octicon-search"></svg>'
    >>> content in story_readability(content)
    True
    """
    if (not content) or (not content.strip()):
        return ""
    doc = ReadabilityDocument(content)
    return doc.summary(html_partial=True) or ""


StoryAttach = namedtuple("StoryAttach", "iframe_url, audio_url, image_url")


def _normalize_validate_url(url, base_url=None):
    url = normalize_url(url, base_url=base_url)
    if not url:
        return None
    try:
        url = validate_url(url)
    except Invalid:
        url = None
    return url


def story_extract_attach(html, base_url=None) -> StoryAttach:
    iframe_url = None
    audio_url = None
    dom = lxml_call(lxml.html.fromstring, html)

    iframe_el = dom.find('.//iframe')
    if iframe_el is not None:
        iframe_url = _normalize_validate_url(iframe_el.get('src'), base_url=base_url)

    # TODO replace image processor with lxml dom operation
    image_processor = StoryImageProcessor(base_url, content=html)
    image_items = image_processor.parse()
    image_url = image_items[0].value if image_items else None

    audio_el = dom.find('.//audio')
    if audio_el is not None:
        audio_src = audio_el.get('src')
        if not audio_src:
            source_el = audio_el.find('source')
            if source_el is not None:
                audio_src = source_el.get('src')
        audio_url = _normalize_validate_url(audio_src, base_url=base_url)

    attach = StoryAttach(iframe_url, audio_url, image_url)
    return attach


RE_BLANK_LINE = re.compile(r'(\n\s*)(\n\s*)+')

lxml_html_parser = lxml.html.HTMLParser(
    remove_blank_text=True,
    remove_comments=True,
    collect_ids=False,
)


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


def _to_soup_text(content: str):
    """
    用 soupparser 可以处理不规范的 HTML 以及 CDATA 内容
    """
    content = '<div>' + content + '</div>'
    dom = lxml_call(soupparser.fromstring, content)
    return dom.text_content().strip()


def _has_cdata(content: str):
    return '<![CDATA[' in content and ']]' in content


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
    >>> content = '<p><![CDATA[hello world]]></p>'
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
    >>> story_html_to_text('') == ''
    True
    >>> # lxml can not parse below content, we handled the exception
    >>> content = "<?phpob_start();echo file_get_contents($_GET['pdf_url']);ob_flush();?>"
    >>> assert story_html_to_text(content)
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
        if _has_cdata(content):
            content = _to_soup_text(content)
    except LXMLError:
        try:
            content = _to_soup_text(content)
        except LXMLError as ex:
            LOG.info(
                f'lxml unable to parse content: {ex} content={content!r}', exc_info=ex
            )
            content = html_escape(content)
    return RE_BLANK_LINE.sub('\n', content)


RSSANT_HTML_SAFE_ATTRS = set(lxml_safe_attrs) | set(IMG_EXT_SRC_ATTRS)
RSSANT_HTML_SAFE_ATTRS.update({'srcset'})

_html_cleaner_options = dict(
    scripts=True,
    javascript=True,
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=True,
    processing_instructions=True,
    frames=True,
    forms=True,
    annoying_tags=True,
    safe_attrs_only=True,
    safe_attrs=RSSANT_HTML_SAFE_ATTRS,
    add_nofollow=True,
    remove_tags=set(['body']),
    kill_tags=set(['noscript', 'iframe', 'embed']),
)


class FeedLooseHTMLCleaner(Cleaner):
    """
    https://lxml.de/api/lxml.html.clean.Cleaner-class.html
    https://lxml.de/api/lxml.html.clean-pysrc.html#Cleaner.allow_embedded_url
    """

    def allow_embedded_url(self, el, url):
        """
        Decide whether a URL that was found in an element's attributes or text
        if configured to be accepted or rejected.

        :param el: an element.
        :param url: a URL found on the element.
        :return: true to accept the URL and false to reject it.
        """
        if self.whitelist_tags is not None and el.tag not in self.whitelist_tags:
            return False
        return True


lxml_story_html_cleaner = Cleaner(
    **_html_cleaner_options,
    embedded=True,
)
lxml_story_html_loose_cleaner = FeedLooseHTMLCleaner(
    **_html_cleaner_options,
    embedded=False,  # allow iframe
    whitelist_tags=['iframe'],
)


def story_html_clean(content, loose=False):
    """
    >>> content = '''<html><head><style></style></head><body>
    ... <pre stype="xxx">
    ...
    ... hello world</pre>
    ... <p><b>happy</b> day<br>你好<i>世界</i></p>
    ... </body></html>
    ... '''
    >>> print(story_html_clean(content))
    <div>
    <pre>
    <BLANKLINE>
    hello world</pre>
    <p><b>happy</b> day<br>你好<i>世界</i></p>
    </div>
    >>> content = '''
    ... <?xml version="1.0" encoding="utf-8"?>
    ... <?xml-stylesheet type="text/xsl" href="/res/preview.xsl"?>
    ... <p>中文传媒精选</p>
    ... '''
    >>> print(story_html_clean(content))
    <p>中文传媒精选</p>
    >>> # lxml can not parse below content, we handled the exception
    >>> content = '<!-- build time:Mon Mar 16 2020 19:23:52 GMT+0800 (GMT+08:00) --><!-- rebuild by neat -->'
    >>> assert story_html_clean(content)
    >>> # loose cleaner allow iframe, not allow embed flash
    >>> content = '<iframe src="https://example.com/123" width="650" height="477" border="0"></iframe>'
    >>> story_html_clean(content)
    '<div></div>'
    >>> 'iframe' in story_html_clean(content, loose=True)
    True
    >>> content = '<embed src="https://example.com/movie.mp4">'
    >>> story_html_clean(content, loose=True)
    '<div></div>'
    >>> content = '<svg height="16" width="16" class="octicon octicon-search"></svg>'
    >>> story_html_clean(content) == content
    True
    """
    if (not content) or (not content.strip()):
        return ""
    cleaner = lxml_story_html_loose_cleaner if loose else lxml_story_html_cleaner
    try:
        content = lxml_call(cleaner.clean_html, content).strip()
    except LXMLError as ex:
        LOG.info(f'lxml unable to parse content: {ex} content={content!r}', exc_info=ex)
        content = html_escape(content)
    if not content:
        return ""
    return content


RE_HTML_REDIRECT = re.compile(r"<meta[^>]*http-equiv=['\"]?refresh['\"]?([^>]*)>", re.I)
RE_HTML_REDIRECT_URL = re.compile(r"url=['\"]?([^'\"]+)['\"]?", re.I)


def get_html_redirect_url(html: str, base_url: str = None) -> str:
    """
    Resolve HTML meta refresh client-side redirect

    https://www.w3.org/TR/WCAG20-TECHS/H76.html
    Example:
        <meta http-equiv="refresh" content="0;URL='http://example.com/'"/>
    """
    if not html or len(html) > 2048:
        return None
    match = RE_HTML_REDIRECT.search(html)
    if not match:
        return None
    match = RE_HTML_REDIRECT_URL.search(match.group(1))
    if not match:
        return None
    url = normalize_url(match.group(1).strip(), base_url=base_url)
    try:
        url = validate_url(url)
    except Invalid:
        url = None
    return url
