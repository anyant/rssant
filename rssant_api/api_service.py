import logging
import socket

import httpx
import requests
from validr import T

from rssant_api.models import STORY_SERVICE, Feed
from rssant_api.models.worker_task import (
    WorkerTask,
    WorkerTaskExpired,
    WorkerTaskPriority,
)
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
        task_obj_s = []
        api = 'worker_rss.find_feed'
        for item in item_s:
            key = '{}:{}'.format(api, item['feed_creation_id'])
            task_obj = WorkerTask.from_dict(
                api=api,
                key=key,
                data=item,
                priority=WorkerTaskPriority.FIND_FEED,
                expired_seconds=WorkerTaskExpired.FIND_FEED,
            )
            task_obj_s.append(task_obj)
        WorkerTask.bulk_save(task_obj_s)


API_SERVICE = RssantApiService()
