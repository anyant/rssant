import logging
import random
from threading import RLock
from typing import Optional

from rssant_api.models import Feed, FeedCreation, FeedStatus, WorkerTask
from rssant_api.models.worker_task import WorkerTaskExpired, WorkerTaskPriority
from rssant_common.base64 import UrlsafeBase64
from rssant_common.throttle import throttle
from rssant_config import CONFIG

LOG = logging.getLogger(__name__)

CHECK_FEED_SECONDS = CONFIG.check_feed_minutes * 60


class RssantTaskService:
    def __init__(self) -> None:
        self._cache = []
        self._lock = RLock()

    def _add_task(self, task: WorkerTask):
        self._cache.append(task)

    def _bulk_save_task(self):
        if self._cache:
            task_s = self._cache
            self._cache = []
            WorkerTask.bulk_save(task_s)
            return True
        return False

    def _pick_task(self) -> Optional[WorkerTask]:
        return WorkerTask.poll()

    def get(self):
        with self._lock:
            task = self._pick_task()
            if task is not None:
                return task
            self._fetch_sync_feed_task()
            self._fetch_find_feed_task()
            has_task = self._bulk_save_task()
            if not has_task:
                return None
            task = self._pick_task()
            return task

    @throttle(seconds=10)
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
            task = WorkerTask.from_dict(
                api=task_api,
                key=task_key,
                data=task_data,
                priority=WorkerTaskPriority.SYNC_FEED,
                expired_seconds=WorkerTaskExpired.SYNC_FEED,
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
            task = WorkerTask.from_dict(
                api=task_api,
                key=task_key,
                data=task_data,
                priority=WorkerTaskPriority.RETRY_FIND_FEED,
                expired_seconds=WorkerTaskExpired.RETRY_FIND_FEED,
            )
            self._add_task(task)

    @throttle(seconds=60)
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
