import pytest
from pytest_httpserver import HTTPServer

from rssant_feedlib.finder import FeedFinder


home_page = """
<html lang="zh-CN">
<head>
<link rel="stylesheet" type="text/css" media="screen" href="/static/css/style.css?v=542593bb5a5bdab56df4173323d215e9" />
<link rel="shortcut icon" href="/static/img/icon_rayps_64.png" type="image/png" />
<link rel="canonical" href="/t/597557" />
<link rel="alternate" type="application/atom+xml" title="Bad Feed" href="/bad-feed.xml" />
<link type="text/xml" title="OK Feed" href="/ok-feed.xml" />
<script type="text/javascript" src="/blog/mt.js"></script>
<title>阮一峰的网络日志</title>
<head>
<body>
这里记录每周值得分享的科技内容，周五发布。
</body>
</html>
"""

bad_feed_page = """
<title>EGOIST v1-legacy</title>
"""

ok_feed_page = """
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Test-Feed-Finder</title>
<subtitle>way to explore</subtitle>


<link rel="alternate" type="text/html" href="https://www.v2ex.com/" />
<link rel="self" type="application/atom+xml" href="https://www.v2ex.com/feed/member/guyskk.xml" />
<id>https://www.v2ex.com/</id>

<updated>2017-08-31T11:31:21Z</updated>

<rights>Copyright © 2010-2018, V2EX</rights>
<entry>
    <title>[Python] 最近看异步 IO，发现 curio 真是好，看了一半感觉豁然开朗</title>
    <link rel="alternate" type="text/html" href="https://www.v2ex.com/t/387702#reply0" />
    <id>tag:www.v2ex.com,2017-09-02:/t/387702</id>
    <published>2017-09-02T11:34:21Z</published>
    <updated>2017-08-31T11:31:21Z</updated>
    <author>
        <name>guyskk</name>
        <uri>https://www.v2ex.com/member/guyskk</uri>
    </author>
    <content type="html" xml:base="https://www.v2ex.com/" xml:lang="en"><![CDATA[
    GitHub： <a target="_blank" href="https://github.com/dabeaz/curio" rel="nofollow">https://github.com/dabeaz/curio</a>
<br />开发文档（推荐）： <a target="_blank" href="http://curio.readthedocs.io/en/latest/devel.html" rel="nofollow">
http://curio.readthedocs.io/en/latest/devel.html</a>
<br />
<br />初步看了一下源码，在架构设计上完胜 asyncio，也有很多关于实现细节的注解，对理解异步 IO 大有帮助。
    ]]></content>
</entry><entry>
    <title>[分享创造] 发现 unicode 有点意思ௐ，用来做 icon 怎么样ൠ？</title>
    <link rel="alternate" type="text/html" href="https://www.v2ex.com/t/378265#reply5" />
    <id>tag:www.v2ex.com,2017-07-27:/t/378265</id>
    <published>2017-07-27T02:27:43Z</published>
    <updated>2017-07-27T07:12:31Z</updated>
    <author>
        <name>guyskk</name>
        <uri>https://www.v2ex.com/member/guyskk</uri>
    </author>
    <content type="html" xml:base="https://www.v2ex.com/" xml:lang="en"><![CDATA[
    偶然发现一个网站介绍了所有的 unicode 码，里面非常多好玩的符号
<br /><a target="_blank" href="http://graphemica.com/unicode/characters/page/13" rel="nofollow">
http://graphemica.com/unicode/characters/page/13</a>
    ]]></content>
</entry>
</feed>
"""


invalid_url_page = """
<HTML>
<HEAD><TITLE>404 Not Found</TITLE></HEAD>
<BODY BGCOLOR="#cc9999" TEXT="#000000" LINK="#2020ff" VLINK="#4040cc">
<H4>404 Not Found</H4>
File not found.
<HR>
<ADDRESS><A HREF="http://...">The Super Encoder & Transcoder.</A></ADDRESS>
</BODY>
</HTML>
"""


def _setup_feed_server(httpserver: HTTPServer):
    httpserver.expect_request('/').respond_with_data(home_page)
    httpserver.expect_request('/404').respond_with_data('Not Found', status=404)
    httpserver.expect_request('/invalid-url').respond_with_data(invalid_url_page)
    headers = {'Location': '/302'}
    httpserver.expect_request('/302').respond_with_data(
        '302 Redirect', status=302, headers=headers)
    headers = {'Location': '/'}
    httpserver.expect_request('/go/home').respond_with_data(
        'Go Home', status=301, headers=headers)
    httpserver.expect_request('/ok-feed.xml').respond_with_data(ok_feed_page)
    httpserver.expect_request('/bad-feed.xml').respond_with_data(bad_feed_page)


def _create_finder(start_url):
    messages = []

    def message_handler(msg):
        messages.append(msg)

    finder = FeedFinder(
        start_url,
        message_handler=message_handler,
        allow_private_address=True,
    )
    return finder, messages


@pytest.mark.parametrize('start_path', [
    '/',
    '/404',
    '/invalid-url',
    '/302',
    '/go/home',
    '/bad-feed.xml',
    '/ok-feed.xml',
])
def test_find_ok(httpserver: HTTPServer, start_path: str):
    _setup_feed_server(httpserver)
    start_url = httpserver.url_for(start_path)
    feed_url = httpserver.url_for('/ok-feed.xml')
    finder, messages = _create_finder(start_url)
    with finder:
        found = finder.find()
    assert found, f'messages={messages}'
    response, raw_result = found
    assert response.url == feed_url
    assert raw_result.feed['title'] == 'Test-Feed-Finder'


def test_find_not_found(httpserver: HTTPServer):
    httpserver.expect_request('/404').respond_with_data('Not Found', status=404)
    start_url = httpserver.url_for('/404')
    finder, messages = _create_finder(start_url)
    with finder:
        found = finder.find()
        assert not found, f'messages={messages}'


real_urls = [
    "ruanyifeng.com",
    "https://arp242.net/feed.xml",
    "https://www.imququ.com",
    "blog.guyskk.com",
    "http://www.zreading.cn/ican/2010/03/feed-subscribe/",
    "http://www.ruanyifeng.com/blog/",
    "https://www.zhihu.com/question/19580096",
]


@pytest.mark.xfail(run=False, reason='depends on test network')
@pytest.mark.parametrize('start_url', real_urls)
def test_find_real(start_url: str):
    finder, messages = _create_finder(start_url)
    with finder:
        found = finder.find()
        if found:
            response, result = found
            print(f"Got: response={response} result={result}")
        assert found, messages
