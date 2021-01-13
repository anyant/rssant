from pathlib import Path

import pytest
from rssant_feedlib.fulltext import (
    split_sentences, is_summary,
    decide_accept_fulltext, FulltextAcceptStrategy,
    is_fulltext_content, StoryContentInfo,
)
from rssant_feedlib.processor import (
    story_readability, story_html_clean, process_story_links,
)

_data_dir = Path(__file__).parent / 'testdata/fulltext'


sentence_text = (
    '标点符号是书面上用于标明句读和语气的符号。\r\n'
    '标点符号是辅助文字记录语言的符号，是书面语的组成部分，用来表示停顿、语气以及词语的性质和作用。\n'
    '句子，前后都有停顿，并带有一定的句调，表示相对完整的意义。句子前后或中间的停顿，'
    '在口头语言中，表现出来就是时间间隔，在书面语言中，就用标点符号来表示。\r\n'
    'A Hello, World! program generally is a computer program that outputs or '
    'displays the message "Hello, World!". Such a program is very simple '
    'in most programming languages, and is often used to illustrate the '
    'basic syntax of a programming language. It is often the first program '
    'written by people learning to code.\n'
)


def test_split_sentences():
    sentences = split_sentences(sentence_text)
    assert len(sentences) == 17, sentences


def test_split_link_sentences():
    text = (
        '这是一个 Google 的链接https://www.google.com和一个蚁阅的链接'
        'https://rss.anyant.com/feed?id=1234。\n'
        'I like read RSS!'
    )
    sentences = split_sentences(text)
    assert len(sentences) == 3, sentences
    assert sentences[0] == '这是一个 Google 的链接'
    assert sentences[1] == '和一个蚁阅的链接'
    assert sentences[2] == 'I like read RSS'


def test_split_number_sentences():
    text = (
        '中国移动的号码是10086，短信价格是0.1元/条'
    )
    sentences = split_sentences(text)
    assert len(sentences) == 2, sentences
    assert sentences[0] == '中国移动的号码是'
    assert sentences[1] == '短信价格是'


def test_split_short_sentences():
    text = (
        '这是一句话。'
        '短句\n'
        'short.'
        'I like read RSS!'
        '。。。'
    )
    sentences = split_sentences(text)
    assert len(sentences) == 2, sentences
    assert sentences[0] == '这是一句话'
    assert sentences[1] == 'I like read RSS'
    assert split_sentences('hello') == []
    assert split_sentences('你好，世界') == []


thoughtworks_subtext = """
从项目制到产品制，说来只有6个本质不同，但实施起来并非易事。所有产品都需要每两周发布一次吗？不管什么类型的产品，都需要投入大量精力做用户研究吗？是否每个产品都应有一致的增长和运营策略？哪些产品做“虚拟”端到端团队就够了、哪些产品必须有“实体”的端到端团队？怎样才算够“产品化”了？
从项目制到产品制之2：产品化运作的成熟度最先出现在ThoughtWorks洞见。
"""

thoughtworks_fulltext = (_data_dir / 'thoughtworks.txt').read_text()


def test_is_summary():
    assert not is_summary('', '')
    assert is_summary('', 'hello world')
    assert not is_summary('hello world', 'hello')
    assert is_summary(thoughtworks_subtext, thoughtworks_fulltext)
    assert not is_summary(thoughtworks_subtext, thoughtworks_subtext)
    assert not is_summary(thoughtworks_fulltext, thoughtworks_subtext)


rssant_fulltext = """
蚁阅更新日志
支持PWA PWA 全称叫渐进式 Web 应用程序，可以让网站像 App 一样添加到桌面，具有沉浸式的用户体验。
这项技术比较新，许多浏览器还不完全支持，所以蚁阅默认没有开启这个功能。
打开方式： 使用 Chrome，Safari，火狐，小米浏览器等支持 PWA 的浏览器访问蚁阅。
点击蚁阅右上角头像进入设置页面，开启 PWA 模式。
浏览器可能会弹出【将蚁阅添加到桌面】的提示，点击确认即可。
如果没有弹出提示，可以从浏览器菜单里，手动将蚁阅添加到桌面。
安卓系统上，浏览器可能需要【桌面快捷方式】权限，可以在系统设置中授权。
如果使用中遇到问题，可尝试关闭PWA模式，或者清除缓存和...
"""

rssant_subtext_1 = """
蚁阅更新日志
使用邮箱注册的用户，可在设置页面绑定GitHub账号，绑定后系统会自动获取GitHub头像。
"""

rssant_subtext_2 = """
蚁阅更新日志
修复了 RSS 代理的一些问题。
"""


def test_is_not_summary():
    assert not is_summary(rssant_subtext_1, rssant_fulltext)
    assert not is_summary(rssant_subtext_2, rssant_fulltext)
    assert not is_summary(rssant_fulltext, rssant_subtext_1)
    assert not is_summary(rssant_fulltext, rssant_subtext_2)


def _clean_story_html(text, *, readability=False):
    text = story_html_clean(text)
    if readability:
        text = story_readability(text)
    url = 'https://rss.anyant.com/'
    text = process_story_links(text, url)
    return text


@pytest.mark.parametrize('name, expect_is_summary', [
    ('hackernews1', False),
    ('hackernews2', True),
    ('haibaomanhua', False),
    ('juejin1', True),
    ('juejin2', True),
    ('woshipm', False),
    ('xkcd', True),
    ('martinfowler', True),
])
def test_rss_is_summary(name, expect_is_summary):
    rss_html = (_data_dir / f'{name}_rss.html').read_text()
    rss_html = _clean_story_html(rss_html)
    web_html = (_data_dir / f'{name}_web.html').read_text()
    web_html = _clean_story_html(web_html, readability=True)
    rss_text = StoryContentInfo(rss_html).text
    web_text = StoryContentInfo(web_html).text
    got = is_summary(rss_text, web_text)
    assert got == expect_is_summary


@pytest.mark.parametrize('name, expect_is_fulltext', [
    ('hackernews1_rss', False),
    ('hackernews1_web', True),
    ('haibaomanhua_rss', True),
    ('haibaomanhua_web', False),
    ('juejin1_rss', False),
    ('juejin1_web', True),
    ('woshipm_rss', True),
    ('woshipm_web', True),
    ('xkcd_rss', False),
    ('xkcd_web', False),
    ('martinfowler_rss', False),
    ('martinfowler_web', True),
])
def test_is_fulltext_content(name, expect_is_fulltext):
    html = (_data_dir / f'{name}.html').read_text()
    html = _clean_story_html(html, readability=name.endswith('web'))
    got = is_fulltext_content(StoryContentInfo(html))
    assert got == expect_is_fulltext


@pytest.mark.parametrize('name, expect_accept', [
    ('hackernews1', FulltextAcceptStrategy.APPEND),
    ('hackernews2', FulltextAcceptStrategy.APPEND),
    ('haibaomanhua', FulltextAcceptStrategy.REJECT),
    ('juejin1', FulltextAcceptStrategy.REPLACE),
    ('juejin2', FulltextAcceptStrategy.REPLACE),
    ('woshipm', FulltextAcceptStrategy.REJECT),
    ('xkcd', FulltextAcceptStrategy.APPEND),
    ('martinfowler', FulltextAcceptStrategy.APPEND),
])
def test_decide_accept_fulltext(name, expect_accept):
    rss_html = (_data_dir / f'{name}_rss.html').read_text()
    rss_html = _clean_story_html(rss_html)
    web_html = (_data_dir / f'{name}_web.html').read_text()
    web_html = _clean_story_html(web_html, readability=True)
    got_accept = decide_accept_fulltext(
        StoryContentInfo(web_html), StoryContentInfo(rss_html))
    assert got_accept == expect_accept
