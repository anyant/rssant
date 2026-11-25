import json

import pytest
from django.contrib.auth.models import User
from django.test import Client, TestCase
from django.utils import timezone

from rssant_api.helper import reverse_url
from rssant_api.models import (
    Feed,
    FeedCreation,
    FeedStatus,
    FeedUrlMap,
    RsshubInstance,
)
from rssant_config import CONFIG
from rssant_harbor.harbor_service import HARBOR_SERVICE
from rssant_harbor.schema import validate_feed_output

RSSHUB_URL = 'https://rsshub.app/v2ex/topics/hot'


def make_feed(url=RSSHUB_URL, **kwargs):
    now = timezone.now()
    fields = dict(
        url=url,
        status=FeedStatus.READY,
        title='test',
        dt_updated=now,
        reverse_url=reverse_url(url),
    )
    fields.update(kwargs)
    return Feed.objects.create(**fields)


@pytest.mark.dbtest
class RsshubInstanceTestCase(TestCase):
    def test_cooldown_lifecycle(self):
        url = 'https://rss.wudifeixue.com'
        until = RsshubInstance.set_cooldown(url, 60)
        assert RsshubInstance.cooling_urls() == {url}
        assert until > timezone.now()
        RsshubInstance.clear_cooldown(url)
        assert RsshubInstance.cooling_urls() == set()

    def test_cooldown_capped_at_30_minutes(self):
        url = 'https://rsshub.gneko.io'
        until = RsshubInstance.set_cooldown(url, 24 * 60 * 60)
        seconds = (until - timezone.now()).total_seconds()
        assert 29 * 60 < seconds <= 30 * 60

    def test_expired_cooldown_not_included(self):
        url = 'https://rsshub.rss.tips'
        RsshubInstance.set_cooldown(url, 60)
        RsshubInstance.objects.filter(url=url).update(
            cooldown_until=timezone.now() - timezone.timedelta(seconds=1)
        )
        assert RsshubInstance.cooling_urls() == set()


@pytest.mark.dbtest
class FeedCreationRetryTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('retry', password='test123456')

    def _creation(self, url='https://rsshub.app/v2ex/topics/hot'):
        return FeedCreation.objects.create(
            user=self.user, url=url, message='pending'
        )

    def test_retryable_keeps_pending_and_skips_not_found(self):
        creation = self._creation()
        HARBOR_SERVICE.save_feed_creation_result(
            feed_creation_id=creation.id,
            messages=['RSSHub 源暂时取不到内容，已排入自动重试'],
            feed=None,
            retryable=True,
        )
        creation.refresh_from_db()
        assert creation.status == FeedStatus.PENDING
        assert creation.deleted_at is None
        assert FeedUrlMap.find_target(creation.url) is None

    def test_permanent_failure_sets_error_and_not_found(self):
        creation = self._creation('https://rsshub.app/not/exists')
        HARBOR_SERVICE.save_feed_creation_result(
            feed_creation_id=creation.id,
            messages=['RSSHub 路由不存在（404）'],
            feed=None,
            retryable=False,
        )
        creation.refresh_from_db()
        assert creation.status == FeedStatus.ERROR
        assert FeedUrlMap.find_target(creation.url) == FeedUrlMap.NOT_FOUND

    def test_retry_query_uses_last_attempt_time(self):
        old = timezone.now() - timezone.timedelta(hours=5)
        retried = self._creation('https://rsshub.app/a')
        FeedCreation.objects.filter(pk=retried.pk).update(
            dt_created=old, dt_updated=timezone.now()
        )
        waiting = self._creation('https://rsshub.app/b')
        FeedCreation.objects.filter(pk=waiting.pk).update(
            dt_created=old, dt_updated=old
        )
        id_urls = FeedCreation.query_id_urls_by_status(
            FeedStatus.PENDING, survival_seconds=4 * 60 * 60
        )
        assert [x[0] for x in id_urls] == [waiting.id]


@pytest.mark.dbtest
class FeedRsshubStateTestCase(TestCase):
    def test_update_feed_info_sets_offset_and_cools_instance(self):
        feed = make_feed()
        url = 'https://rss.wudifeixue.com'
        HARBOR_SERVICE.update_feed_info(
            feed_id=feed.id,
            feed=dict(status=FeedStatus.ERROR, warnings='RSSHub 实例暂不可用'),
            rsshub_offset=3,
            rsshub_cooling=[dict(url=url, cooldown_seconds=60)],
        )
        feed.refresh_from_db()
        assert feed.rsshub_offset == 3
        assert feed.status == FeedStatus.ERROR
        assert RsshubInstance.cooling_urls() == {url}

    def test_update_feed_info_endpoint_accepts_cooling(self):
        feed = make_feed()
        client = Client()
        payload = dict(
            feed_id=feed.id,
            feed=dict(status=FeedStatus.ERROR, warnings='RSSHub 实例暂不可用'),
            rsshub_offset=3,
            rsshub_cooling=[
                dict(url='https://rss.wudifeixue.com', cooldown_seconds=60)
            ],
        )
        response = client.post(
            '/api/v1/harbor_rss.update_feed_info',
            data=json.dumps(payload),
            content_type='application/json',
            HTTP_X_RSSANT_SERVICE_SECRET=CONFIG.service_secret,
        )
        assert response.status_code == 204, response.content
        feed.refresh_from_db()
        assert feed.rsshub_offset == 3
        assert RsshubInstance.cooling_urls() == {'https://rss.wudifeixue.com'}

    def test_save_creation_result_endpoint_accepts_retryable(self):
        self.user = User.objects.create_user('retryable', password='test123456')
        creation = FeedCreation.objects.create(
            user=self.user, url='https://rsshub.app/busy', message=''
        )
        client = Client()
        payload = dict(
            feed_creation_id=creation.id,
            messages=['RSSHub 源暂时取不到内容，已排入自动重试'],
            feed=None,
            retryable=True,
        )
        response = client.post(
            '/api/v1/harbor_rss.save_feed_creation_result',
            data=json.dumps(payload),
            content_type='application/json',
            HTTP_X_RSSANT_SERVICE_SECRET=CONFIG.service_secret,
        )
        assert response.status_code == 204, response.content
        creation.refresh_from_db()
        assert creation.status == FeedStatus.PENDING

    def test_take_outdated_returns_rsshub_state(self):
        feed = make_feed(
            rsshub_instance='https://rss.wudifeixue.com',
            rsshub_offset=6,
            dt_checked=None,
        )
        rows = Feed.take_outdated_feeds(300, limit=100)
        row = [x for x in rows if x['feed_id'] == feed.id][0]
        assert row['rsshub_instance'] == 'https://rss.wudifeixue.com'
        assert row['rsshub_offset'] == 6

    def test_update_feed_keeps_url_and_clears_cooldown(self):
        feed = make_feed()
        old_reverse_url = feed.reverse_url
        instance = 'https://rsshub.rss.tips'
        RsshubInstance.set_cooldown(instance, 60)
        feed_dict = dict(
            url=RSSHUB_URL,
            use_proxy=False,
            title='test',
            content_length=10,
            content_hash_base64='hash',
            storys=[],
            rsshub_instance=instance,
        )
        HARBOR_SERVICE.update_feed(
            feed_id=feed.id, feed=validate_feed_output(feed_dict)
        )
        feed.refresh_from_db()
        # 换实例抓取成功后，订阅地址和 reverse_url 都不变
        assert feed.url == RSSHUB_URL
        assert feed.reverse_url == old_reverse_url
        assert feed.rsshub_instance == instance
        assert RsshubInstance.cooling_urls() == set()
        assert feed.status == FeedStatus.READY
