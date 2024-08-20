import base64
import logging
import random
import time
from threading import RLock
from typing import Optional

from rssant_api.models import Feed, FeedCreation, FeedStatus
from rssant_common.base64 import UrlsafeBase64
from rssant_config import CONFIG

LOG = logging.getLogger(__name__)

CHECK_FEED_SECONDS = CONFIG.check_feed_minutes * 60


class RssantTask:
    def __init__(
        self,
        *,
        api: str,
        key: str,
        data: dict,
        priority: int,
        timestamp: int,
    ) -> None:
        self.api = api
        self.key = key
        self.data = data
        self.priority = priority
        self.timestamp = timestamp

    def to_dict(self):
        return dict(
            api=self.api,
            key=self.key,
            data=self.data,
            priority=self.priority,
            timestamp=self.timestamp,
        )


class RssantTaskService:
    def __init__(self) -> None:
        self._cache = []
        self._lock = RLock()

    def _add_task(self, task: RssantTask):
        self._cache.append(task)

    def _pick_task(self) -> Optional[RssantTask]:
        if not self._cache:
            return None
        return self._cache.pop(0)

    def get(self):
        with self._lock:
            task = self._pick_task()
            if task is not None:
                return task
            # 限制数据库查询频率，避免频繁查询
            self._fetch_sync_feed_task()
            self._fetch_find_feed_task()
            task = self._pick_task()
            return task

    def _b64encode(self, data: Optional[bytes]):
        if data is None:
            return None
        return base64.urlsafe_b64encode(data).decode('ascii')

    def _fetch_sync_feed_task(self):
        rand_sec = random.random() * CHECK_FEED_SECONDS / 10
        outdate_seconds = CHECK_FEED_SECONDS + rand_sec
        feeds = Feed.take_outdated_feeds(outdate_seconds, limit=100)
        LOG.info('found {} feeds need sync'.format(len(feeds)))
        for feed in feeds:
            task_api = 'worker_rss.sync_feed'
            task_key = f'{task_api}:{feed["feed_id"]}'
            checksum_data_base64 = UrlsafeBase64.encode(feed['checksum_data'])
            task_data = dict(
                feed_id=feed['feed_id'],
                url=feed['url'],
                etag=feed['etag'],
                last_modified=feed['last_modified'],
                use_proxy=feed['use_proxy'],
                checksum_data_base64=checksum_data_base64,
            )
            task = RssantTask(
                api=task_api,
                key=task_key,
                data=task_data,
                priority=1,
                timestamp=int(time.time()),
            )
            self._add_task(task)

    def _retry_feed_creations(self, feed_creation_id_urls):
        feed_creation_ids = [id for (id, _) in feed_creation_id_urls]
        FeedCreation.bulk_set_pending(feed_creation_ids)
        for feed_creation_id, url in feed_creation_id_urls:
            task_api = 'worker_rss.find_feed'
            task_key = f'{task_api}:{feed_creation_id}'
            task_data = dict(
                feed_creation_id=feed_creation_id,
                url=url,
            )
            task = RssantTask(
                api=task_api,
                key=task_key,
                data=task_data,
                priority=1,
                timestamp=int(time.time()),
            )
            self._add_task(task)

    def _fetch_find_feed_task(self):
        # 重试 status=UPDATING 超过4小时的订阅
        feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
            FeedStatus.UPDATING, survival_seconds=4 * 60 * 60
        )
        num_retry_updating = len(feed_creation_id_urls)
        LOG.info('retry {} status=UPDATING feed creations'.format(num_retry_updating))
        self._retry_feed_creations(feed_creation_id_urls)
        # 重试 status=PENDING 超过4小时的订阅
        feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
            FeedStatus.PENDING, survival_seconds=4 * 60 * 60
        )
        num_retry_pending = len(feed_creation_id_urls)
        LOG.info('retry {} status=PENDING feed creations'.format(num_retry_pending))
        self._retry_feed_creations(feed_creation_id_urls)


TASK_SERVICE = RssantTaskService()
