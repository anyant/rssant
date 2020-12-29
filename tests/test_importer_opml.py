import os.path
from rssant_feedlib.importer import import_feed_from_text


def _read_sample(filename):
    with open(os.path.join('tests/sample/', filename)) as f:
        return f.read()


SAMPLE_STRINGER = _read_sample('stringer.opml')
SAMPLE_INOREADER = _read_sample('inoreader.xml')


def test_parse_opml_stringer():
    expect_items = [
        {'title': "wklken's blog", 'type': 'rss', 'url': 'http://www.wklken.me/feed.xml'},
        {'title': '始终', 'type': 'rss', 'url': 'https://liam0205.me/atom.xml'},
        {'title': '云风的 BLOG', 'type': 'rss', 'url': 'https://blog.codingnow.com/'},
        {'title': '酷 壳 – CoolShell', 'type': 'rss', 'url': 'http://coolshell.cn/feed'},
        {'title': 'Hacker News', 'type': 'rss', 'url': 'https://news.ycombinator.com/rss'},
    ]
    expect_items = {x['url']: x for x in expect_items}
    feed_items = import_feed_from_text(SAMPLE_STRINGER)
    assert len(feed_items) == 5
    for item in feed_items:
        expect = expect_items[item['url']]
        assert item['url'] == expect['url']
        assert item['title'] == expect['title']


def test_parse_opml_inoreader():
    expect_items = [
        {'title': "Guyskk的博客", 'group': '', 'url': 'http://blog.guyskk.com/feed.xml'},
        {'title': "腾讯CDC", 'group': '设计', 'url': 'http://cdc.tencent.com/feed/'},
        {'title': 'Rologo 标志共和国', 'group': '设计', 'url': 'http://www.rologo.com/feed'},
    ]
    expect_items = {x['url']: x for x in expect_items}
    feed_items = import_feed_from_text(SAMPLE_INOREADER)
    assert len(feed_items) == 3
    for item in feed_items:
        expect = expect_items[item['url']]
        assert item['url'] == expect['url']
        assert item['title'] == expect['title']
        assert item['group'] == expect['group']
