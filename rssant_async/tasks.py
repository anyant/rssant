import logging
import time
import asyncio
import concurrent.futures
from collections import OrderedDict

from rssant_feedlib.async_reader import AsyncFeedReader

from .callback_client import CallbackClient


LOG = logging.getLogger(__name__)


class FixSizeOrderedDict(OrderedDict):
    def __init__(self, *args, maxlen=0, **kwargs):
        self._maxlen = maxlen
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        if self._maxlen > 0:
            if len(self) > self._maxlen:
                self.popitem(False)


STORYS_BUFFER = FixSizeOrderedDict(maxlen=10 * 1000)


async def fetch_story(id, url, callback_url=None):
    LOG.info(f'fetch story#{id} url={url} begin')
    async with AsyncFeedReader() as reader:
        status, response = await reader.read(url)
    if response and response.url:
        url = str(response.url)
    story = dict(
        id=id,
        url=url,
        status=status,
    )
    if response:
        story.update(
            encoding=response.rssant_encoding,
            text=response.rssant_text,
        )
    STORYS_BUFFER[id] = story
    LOG.info(f'fetch story#{id} url={url} status={status} finished')
    await CallbackClient.send(callback_url, {'id': id, "url": url, "status": status})


async def detect_story_images(story_id, story_url, image_urls, callback_url=None):
    LOG.info(f'detect story images story_id={story_id} num_images={len(image_urls)} begin')
    async with AsyncFeedReader() as reader:
        async def _read(url):
            status, response = await reader.read(
                url,
                referer="https://rss.anyant.com/story/",
                ignore_content=True
            )
            return url, status
        futs = []
        for url in image_urls:
            futs.append(asyncio.ensure_future(_read(url)))
        t_begin = time.time()
        try:
            results = await asyncio.gather(*futs)
        except (TimeoutError, concurrent.futures.TimeoutError):
            results = [fut.result() for fut in futs if fut.done()]
        cost_ms = (time.time() - t_begin) * 1000
    num_ok = num_error = 0
    images = []
    for url, status in results:
        if status == 200:
            num_ok += 1
        else:
            num_error += 1
        images.append(dict(url=url, status=status))
    LOG.info(f'detect story images story_id={story_id} '
             f'num_images={len(image_urls)} finished, '
             f'ok={num_ok} error={num_error} cost={cost_ms:.0f}ms')
    await CallbackClient.send(callback_url, {
        'story': {'id': story_id, 'url': story_url},
        'images': images
    })
