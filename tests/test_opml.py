import os.path
from rssant_feedlib.opml import parse_opml


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
    data = parse_opml(SAMPLE_STRINGER)
    assert data['title'] == 'Feeds from Stringer'
    assert len(data['items']) == 5
    for item in data['items']:
        assert item == expect_items[item['url']]


def test_parse_opml_inoreader():
    expect_items = [
        {'title': "腾讯CDC", 'type': 'rss', 'url': 'http://cdc.tencent.com/feed/'},
        {'title': 'Rologo 标志共和国', 'type': 'rss', 'url': 'http://www.rologo.com/feed'},
    ]
    expect_items = {x['url']: x for x in expect_items}
    data = parse_opml(SAMPLE_INOREADER)
    assert data['title'] == 'Subscriptions from Inoreader [https://www.inoreader.com]'
    assert len(data['items']) == 2
    for item in data['items']:
        assert item == expect_items[item['url']]
