import pytest
from rssant_config import CONFIG
from rssant_feedlib.reader import FeedReader


@pytest.mark.xfail(reason='proxy depends on test network')
@pytest.mark.parametrize('url', [
    'https://www.reddit.com/r/Python.rss',
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCBcRF18a7Qf58cCRy5xuWwQ',
])
def test_read_by_proxy(url):
    with FeedReader(
        rss_proxy_url=CONFIG.rss_proxy_url,
        rss_proxy_token=CONFIG.rss_proxy_token,
    ) as reader:
        response = reader.read(url, use_proxy=True)
    assert response.ok
    assert response.url == url


@pytest.mark.parametrize('url', [
    'https://www.ruanyifeng.com/blog/atom.xml',
    'https://blog.guyskk.com/feed.xml',
])
def test_read(url):
    with FeedReader() as reader:
        response = reader.read(url)
    assert response.ok
    assert response.url == url
