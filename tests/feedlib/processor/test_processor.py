from pathlib import Path

import pytest

from rssant_feedlib.processor import (
    story_readability,
    normalize_url,
    validate_url,
    get_html_redirect_url,
    story_extract_attach,
    story_has_mathjax,
)

_data_dir = Path(__file__).parent.parent / 'testdata/processor'


def _read_text(filename):
    filepath = _data_dir / filename
    return filepath.read_text()


def test_story_readability():
    """
    readability + lxml 4.5.0 has issue:
        readability.readability.Unparseable: IO_ENCODER
    """
    html = _read_text('test_sample.html')
    story_readability(html)


def test_normalize_invalid_url():
    urls_text = _read_text('test_normalize_url.txt')
    urls = list(urls_text.strip().splitlines())
    for url in urls:
        norm_url = normalize_url(url)
        if url.startswith('urn:') or url.startswith('magnet:'):
            assert norm_url == url
        else:
            assert validate_url(norm_url) == norm_url


def test_normalize_url():
    cases = [
        (None, ''),
        ('hello world', 'hello world'),
        ('你好世界', '你好世界'),
        ('2fd1ca54895', '2fd1ca54895'),
        ('www.example.com', 'http://www.example.com'),
        ('://www.example.com', 'http://www.example.com'),
        ('http://example.comblog', 'http://example.com/blog'),
        ('http://example.com//blog', 'http://example.com/blog'),
        ('http://example.com%5Cblog', 'http://example.com/blog'),
        ('http://example.com%5Cblog/hello', 'http://example.com/blog/hello'),
        ('http%3A//www.example.com', 'http://www.example.com'),
        ('http://www.example.com:80', 'http://www.example.com'),
        ('https://www.example.com:443', 'https://www.example.com'),
        ('http://example%5B.]com/x.php?age=23', 'http://example%5B.]com/x.php?age=23'),
        ('http://example%5B.]com', 'http://example%5B.]com'),
        (
            'http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:80/',
            'http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]/'
        ),
        (
            'http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443/',
            'http://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443/'
        ),
        (
            'https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443/',
            'https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]/'
        ),
        (
            'http://www.example.comhttp://www.example.com/hello',
            'http://www.example.com/hello'
        ),
        (
            'http://www.example.com/white space',
            'http://www.example.com/white%20space'
        ),
        (
            'https://www.example.com.cn/test',
            'https://www.example.com.cn/test'
        ),
        (
            'https://www.bmpi.dev/dev/guide-to-serverless',
            'https://www.bmpi.dev/dev/guide-to-serverless'
        ),
    ]
    for url, expect in cases:
        norm = normalize_url(url)
        assert norm == expect, f'url={url!r} normalize={norm!r} expect={expect!r}'


def test_normalize_base_url():
    base_url = 'http://blog.example.com/feed.xml'
    url = '/post/123.html'
    r = normalize_url(url, base_url=base_url)
    assert r == 'http://blog.example.com/post/123.html'
    url = 'post/123.html'
    r = normalize_url(url, base_url=base_url)
    assert r == 'http://blog.example.com/post/123.html'
    url = '/'
    r = normalize_url(url, base_url=base_url)
    assert r == 'http://blog.example.com/'


def test_normalize_quote():
    base = 'http://blog.example.com'
    base_url = 'http://blog.example.com/feed.xml'
    path_s = [
        '/post/2019-01-10-%E5%AF%BB%E6%89%BE-sourcetree-%E6%9B%BF%E4%BB%A3%E5%93%81/',
        '/notes/%E8%9A%81%E9%98%85%E6%80%A7%E8%83%BD%E4%BC%98%E5%8C%96%E8%AE%B0%E5%BD%95',
        '/1034:4671653473616001/4671655702434113',
    ]
    for p in path_s:
        r = normalize_url(p, base_url=base_url)
        assert r == base + p


@pytest.mark.parametrize('filename', [
    'html_redirect/test_html_redirect_1.html',
    'html_redirect/test_html_redirect_2.html',
    'html_redirect/test_html_redirect_3.html',
])
def test_get_html_redirect_url(filename):
    base_url = 'https://blog.example.com'
    expect = 'https://blog.example.com/html-redirect/'
    html = _read_text(filename)
    got = get_html_redirect_url(html, base_url=base_url)
    assert got == expect


def test_story_extract_attach_iframe():
    html = _read_text('test_iframe.html')
    attach = story_extract_attach(html)
    expect = 'https://player.bilibili.com/player.html?aid=75057811'
    assert attach.iframe_url == expect


def test_story_extract_attach_iframe_link():
    html = _read_text('test_iframe_link.html')
    attach = story_extract_attach(html)
    expect = 'https://video.h5.weibo.cn/1034:4671653473616001/4671655702434113'
    assert attach.iframe_url == expect


def test_story_extract_attach_audio():
    html = _read_text('test_audio.html')
    attach = story_extract_attach(html)
    expect = 'https://chtbl.com/track/r.typlog.com/pythonhunter/8417630310_189758.mp3'
    assert attach.audio_url == expect


def test_story_extract_attach_audio_source():
    html = '''
    <div>
    <p><strong>直接播放</strong>:</p>
    <audio controls preload style="width:80%;margin-left:34px">
    <source src="/static/2020-07-12/podcast-rssant-parttime-product.mp3?controls=1" type="audio/mpeg">
    <p>你的浏览器不支持播放音频，你可以
    <a href="/static/2020-07-12/podcast-rssant-parttime-product.mp3?controls=1">
    下载</a>这个音频文件。</p></audio>
    </div>
    '''
    base_url = 'https://blog.guyskk.com'
    attach = story_extract_attach(html, base_url=base_url)
    expect = '/static/2020-07-12/podcast-rssant-parttime-product.mp3?controls=1'
    assert attach.audio_url == base_url + expect


def test_story_has_mathjax():
    has_mathjax_cases = [
        r'$x^{y^z}=(1+{\rm e}^x)^{-2xy^w}$',
        r'$f(x,y,z) = 3y^2z \left( 3+\frac{7x+5}{1+y^2} \right)$',
        r'$1 \over 3$',
        r'$\vec{a} \cdot \vec{b}=0$',
        r'<p>这里 $n$ 是特征',
        r'向量 $\vec x$ 的长度，即特征的维数。',
        r'<code>$v_i$</code> 是长度',
        r'为 $k$ 的向量，与特征 id 对应，称为特征的隐向量。',
        r'`sum_(i=1)^n i^3=((n(n+1))/2)^2`',
        r'<code>`sum_(i=1)^n i^3=((n(n+1))/2)^2`</code>',
    ]
    not_mathjax_cases = [
        r'$10 aaa $10  $10 aaa $10',
        r'$10 $10  $10 $10',
        r'$10.0',
        r'100$ 100$',
        r'console.log($.fn.jquery); window.$;',
        r'$ === jQuery; typeof($);',
        r"$('p,div'); $('p.red,p.green');",
        r"""
        The model of subscription premium audio content is popular in China,
        where Ximalaya, a unicorn consumer audio platform, has a subscription
        feature for $3 monthly that enables users to access over 4000 e-books
        and over 300 premium audio courses or podcasts. Audio content is also
        available a la carte starting at $0.03 per short, serialized book chapter,
        or anywhere from $10 to $45 for paid audio courses.
        """,
        r"""$ shellcheck test.sh
        In test.sh line 4:
        if[ $# -eq 0 ]""",
        r'$ shellcheck if[ $# -eq 0 ]',
        '$x^\n{y^z}$',
        r'$x^{$y^z}$',
        '`x^\n{y^z}`',
        r'```x^{y^z}```',
    ]
    for text in has_mathjax_cases:
        assert story_has_mathjax(text), text
    for text in not_mathjax_cases:
        assert not story_has_mathjax(text), text
