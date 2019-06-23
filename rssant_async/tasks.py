import logging
import time
import asyncio
import concurrent.futures

from rssant_feedlib.async_reader import AsyncFeedReader, FeedResponseStatus
from rssant_feedlib.blacklist import compile_url_blacklist

from .callback_client import CallbackClient
from .redis_dao import REDIS_DAO

LOG = logging.getLogger(__name__)


REFERER_DENY_LIST = """
qpic.cn
qlogo.cn
qq.com
"""

is_referer_deny_url = compile_url_blacklist(REFERER_DENY_LIST)


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
    await REDIS_DAO.set_story(id, story)
    LOG.info(f'fetch story#{id} url={url} status={status} finished')
    await CallbackClient.send(callback_url, {'id': id, "url": url, "status": status})


async def detect_story_images(story_id, story_url, image_urls, callback_url=None):
    LOG.info(f'detect story images story_id={story_id} num_images={len(image_urls)} begin')
    async with AsyncFeedReader() as reader:
        async def _read(url):
            if is_referer_deny_url(url):
                return url, FeedResponseStatus.REFERER_DENY.value
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
