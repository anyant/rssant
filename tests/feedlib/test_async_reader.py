import pytest
from rssant_config import CONFIG
from rssant_feedlib.async_reader import AsyncFeedReader


@pytest.mark.xfail(reason='proxy depends on test network')
@pytest.mark.parametrize('url', [
    'https://www.reddit.com/r/Python.rss',
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCBcRF18a7Qf58cCRy5xuWwQ',
])
@pytest.mark.asyncio
async def test_async_read_by_proxy(url):
    async with AsyncFeedReader(
        rss_proxy_url=CONFIG.rss_proxy_url,
        rss_proxy_token=CONFIG.rss_proxy_token,
    ) as reader:
        status, response = await reader.read(url, use_proxy=True)
    assert status == 200
    assert response.status == 200
    assert str(response.url) == url


@pytest.mark.parametrize('url', [
    'https://www.ruanyifeng.com/blog/atom.xml',
    'https://blog.guyskk.com/feed.xml',
])
@pytest.mark.asyncio
async def test_read(url):
    async with AsyncFeedReader() as reader:
        status, response = await reader.read(url)
    assert status == 200
    assert response.status == 200
    assert str(response.url) == url
