import asyncio
import atexit
import logging
import socket
from concurrent.futures import ThreadPoolExecutor

import httpx
import requests
from validr import T

from rssant_api.models import STORY_SERVICE, Feed
from rssant_common.service_client import SERVICE_CLIENT
from rssant_feedlib.fulltext import (
    FulltextAcceptStrategy,
    StoryContentInfo,
    split_sentences,
)
from rssant_feedlib.response import FeedResponseStatus

LOG = logging.getLogger(__name__)

T_ACCEPT = T.enum(','.join(FulltextAcceptStrategy.__members__))
TIMEOUT_ERRORS = (
    socket.timeout,
    TimeoutError,
    requests.exceptions.Timeout,
    httpx.TimeoutException,
)


class RssantApiService:

    def __init__(self) -> None:
        self._thread_pool = None
        atexit.register(self._on_shutdown)

    def _get_thread_pool(self):
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(max_workers=5)
        return self._thread_pool

    def _on_shutdown(self):
        if self._thread_pool is not None:
            self._thread_pool.shutdown(wait=False)
            self._thread_pool = None

    def sync_story_fulltext(
        self,
        feed_id: T.int,
        offset: T.int,
        timeout: int = None,
    ) -> T.dict(
        feed_id=T.int,
        offset=T.int.min(0),
        use_proxy=T.bool,
        url=T.url,
        response_status=T.int,
        accept=T_ACCEPT,
    ):
        feed = Feed.get_by_pk(feed_id, detail='+use_proxy')
        story = STORY_SERVICE.get_by_offset(feed_id, offset, detail=True)
        assert story, f'story#{feed_id},{offset} not found'
        story_content_info = StoryContentInfo(story.content)
        num_sub_sentences = len(split_sentences(story_content_info.text))
        ret = dict(
            feed_id=feed_id,
            offset=offset,
            url=story.link,
            use_proxy=feed.use_proxy,
            accept=FulltextAcceptStrategy.REJECT.value,
        )
        try:
            result = SERVICE_CLIENT.call(
                'worker_rss.fetch_story',
                dict(
                    url=story.link,
                    use_proxy=feed.use_proxy,
                    feed_id=feed_id,
                    offset=offset,
                    num_sub_sentences=num_sub_sentences,
                ),
                timeout=timeout,
            )
        except TIMEOUT_ERRORS as ex:
            LOG.error(f'Ask worker_rss.fetch_story timeout: {ex}')
            ret.update(response_status=FeedResponseStatus.CONNECTION_TIMEOUT)
            return ret
        else:
            ret.update(
                response_status=result['response_status'],
                use_proxy=result['use_proxy'],
                accept=result['accept'],
            )
        return ret

    def batch_find_feed(self, item_s: list):
        task_s = []
        for item in item_s:
            task = SERVICE_CLIENT.acall('worker_rss.find_feed', item, timeout=120)
            task_s.append(asyncio.create_task(task))
        all_task = asyncio.gather(*task_s, return_exceptions=True)
        asyncio.get_event_loop().run_until_complete(all_task)

    def batch_find_feed_in_thread(self, item_s: list):
        pool = self._get_thread_pool()
        pool.submit(self, self.batch_find_feed, item_s)


API_SERVICE = RssantApiService()
