import json
from unittest.mock import patch

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
    UserFeed,
)
from rssant_config import CONFIG
from rssant_feedlib.rsshub import get_instances, public_hosts
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

    def test_escalate_first_failure_cools_60_seconds(self):
        url = 'https://rsshub.gneko.io'
        RsshubInstance.escalate_cooldown(url)
        row = RsshubInstance.objects.get(url=url)
        seconds = (row.cooldown_until - timezone.now()).total_seconds()
        assert 55 < seconds <= 61

    def test_escalate_does_not_extend_active_cooldown(self):
        url = 'https://rsshub.gneko.io'
        until = RsshubInstance.set_cooldown(url, 600)
        RsshubInstance.escalate_cooldown(url)
        row = RsshubInstance.objects.get(url=url)
        assert abs((row.cooldown_until - until).total_seconds()) < 1

    def test_escalate_after_success_clears_cools_60_seconds(self):
        # 成功清零后（cooldown_until 为 NULL）再失败，仍按首次 60 秒
        url = 'https://rsshub.gneko.io'
        RsshubInstance.set_cooldown(url, 60)
        RsshubInstance.clear_cooldown(url)
        RsshubInstance.escalate_cooldown(url)
        row = RsshubInstance.objects.get(url=url)
        seconds = (row.cooldown_until - timezone.now()).total_seconds()
        assert 55 < seconds <= 61

    def test_escalate_after_expiry_cools_15_minutes(self):
        url = 'https://rsshub.gneko.io'
        RsshubInstance.set_cooldown(url, 1)
        RsshubInstance.objects.filter(url=url).update(
            cooldown_until=timezone.now() - timezone.timedelta(seconds=1)
        )
        RsshubInstance.escalate_cooldown(url)
        row = RsshubInstance.objects.get(url=url)
        seconds = (row.cooldown_until - timezone.now()).total_seconds()
        assert 895 < seconds <= 901


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
        FeedCreation.objects.filter(pk=creation.pk).update(error_message='旧描述')
        HARBOR_SERVICE.save_feed_creation_result(
            feed_creation_id=creation.id,
            messages=['RSSHub 源暂时取不到内容，已排入自动重试'],
            feed=None,
            retryable=True,
        )
        creation.refresh_from_db()
        assert creation.status == FeedStatus.PENDING
        assert creation.deleted_at is None
        assert creation.error_message == ''
        assert FeedUrlMap.find_target(creation.url) is None

    def test_permanent_failure_sets_error_and_not_found(self):
        creation = self._creation('https://rsshub.app/not/exists')
        error_message = 'RSSHub 路由不存在（404），可能已经下线或改名。换一个 RSS 地址试试。'
        HARBOR_SERVICE.save_feed_creation_result(
            feed_creation_id=creation.id,
            messages=['RSSHub 路由不存在（404）'],
            feed=None,
            retryable=False,
            error_message=error_message,
        )
        creation.refresh_from_db()
        assert creation.status == FeedStatus.ERROR
        assert creation.error_message == error_message
        assert FeedUrlMap.find_target(creation.url) == FeedUrlMap.NOT_FOUND

    def test_success_clears_error_message(self):
        creation = FeedCreation.objects.create(
            user=self.user,
            url='https://example.com/feed.xml',
            message='',
            error_message='旧描述',
        )
        feed = dict(
            url='https://example.com/feed.xml',
            use_proxy=False,
            title='test',
            content_hash_base64='hash',
            storys=[],
        )
        with patch.object(HARBOR_SERVICE, 'update_feed'):
            HARBOR_SERVICE.save_feed_creation_result(
                feed_creation_id=creation.id,
                messages=['Feed: version=rss, title=test'],
                feed=feed,
            )
        creation.refresh_from_db()
        assert creation.status == FeedStatus.READY
        assert creation.feed_id
        assert creation.error_message == ''

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
            rsshub_cooling=[dict(url=url, reason='rate_limit', retry_after=60)],
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
                dict(url='https://rss.wudifeixue.com', reason='transport')
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
            messages=[
                'RSSHub 源暂时取不到内容：已尝试 1 个实例后超时。已排入自动重试。'
            ],
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
        assert FeedUrlMap.find_target(creation.url) is None

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


@pytest.mark.dbtest
class RsshubTaskServiceTestCase(TestCase):
    def test_has_available_instances(self):
        from rssant_harbor.task_service import RssantTaskService

        service = RssantTaskService()
        instances = get_instances()
        assert service._has_available_instances() is True
        for url in instances:
            RsshubInstance.set_cooldown(url, 60)
        assert service._has_available_instances() is False
        RsshubInstance.clear_cooldown(instances[0])
        assert service._has_available_instances() is True


@pytest.mark.dbtest
class FeedWakeupTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('wakeup', password='test123456')

    def _feed(self, url, status=FeedStatus.ERROR, dt_checked=None, subscribe=True):
        # 默认 2 小时前检查过，避开唤醒的 min_age_seconds 过滤
        if dt_checked is None:
            dt_checked = timezone.now() - timezone.timedelta(hours=2)
        feed = make_feed(url, status=status, dt_checked=dt_checked)
        if subscribe:
            UserFeed(
                user=self.user, feed=feed, dt_updated=timezone.now()
            ).save()
        return feed

    def test_wakeup_only_error_with_subscriber(self):
        public = self._feed('https://rsshub.app/v2ex/topics/hot')
        private = self._feed('https://vcrsshub.vercel.app/bilibili/user/video/1')
        self._feed('https://rsshub.app/weibo/user/2', subscribe=False)
        self._feed('https://rsshub.app/zhihu/daily', status=FeedStatus.READY)
        self._feed('https://blog.example.com/feed.xml')
        queued = make_feed(
            'https://rsshub.app/queued', status=FeedStatus.ERROR, dt_checked=None
        )
        UserFeed(
            user=self.user, feed=queued, dt_updated=timezone.now()
        ).save()
        feed_ids, remaining = Feed.wakeup_rsshub_feeds(limit=10)
        assert set(feed_ids) == {public.id, private.id}
        assert remaining == 0
        for feed in (public, private):
            feed.refresh_from_db()
            assert feed.dt_checked is None
        queued.refresh_from_db()
        assert queued.dt_checked is None

    def test_take_outdated_excludes_public_hosts(self):
        # 实例全部冷却时不认领公共实例池的源，自建源和其他源照常
        public = make_feed('https://rsshub.app/guard/a', dt_checked=None)
        private = make_feed('https://my-rsshub.example.com/guard', dt_checked=None)
        plain = make_feed('https://blog.example.com/feed.xml', dt_checked=None)
        feed_ids = Feed.take_outdated(
            300, limit=10, exclude_hosts=public_hosts()
        )
        assert public.id not in feed_ids
        assert private.id in feed_ids
        assert plain.id in feed_ids
        public.refresh_from_db()
        assert public.dt_checked is None

    def test_take_outdated_without_exclude_claims_public_hosts(self):
        public = make_feed('https://rsshub.app/guard/a', dt_checked=None)
        feed_ids = Feed.take_outdated(300, limit=10)
        assert public.id in feed_ids

    def test_wakeup_orders_by_dt_checked_and_respects_limit(self):
        now = timezone.now()
        old = self._feed(
            'https://rsshub.app/a', dt_checked=now - timezone.timedelta(hours=3)
        )
        mid = self._feed(
            'https://rsshub.app/b', dt_checked=now - timezone.timedelta(hours=2)
        )
        self._feed(
            'https://rsshub.app/c', dt_checked=now - timezone.timedelta(hours=1)
        )
        feed_ids, remaining = Feed.wakeup_rsshub_feeds(limit=2)
        assert feed_ids == [old.id, mid.id]
        assert remaining == 1

    def test_wakeup_only_takes_feeds_checked_before_run_start(self):
        cutoff = timezone.now() - timezone.timedelta(minutes=5)
        old = self._feed(
            'https://rsshub.app/old',
            dt_checked=cutoff - timezone.timedelta(hours=1),
        )
        recent = self._feed(
            'https://rsshub.app/recent',
            dt_checked=cutoff + timezone.timedelta(minutes=1),
        )
        feed_ids, _ = Feed.wakeup_rsshub_feeds(limit=10, checked_before=cutoff)
        assert feed_ids == [old.id]
        recent.refresh_from_db()
        assert recent.dt_checked is not None

    def test_wakeup_does_not_repeat_within_run(self):
        run_start = timezone.now()
        feed = self._feed(
            'https://rsshub.app/once',
            dt_checked=run_start - timezone.timedelta(hours=1),
        )
        feed_ids, _ = Feed.wakeup_rsshub_feeds(limit=10, checked_before=run_start)
        assert feed.id in feed_ids
        # 唤醒后失败，dt_checked 刷新到本次运行期间
        Feed.objects.filter(id=feed.id).update(
            dt_checked=timezone.now(), status=FeedStatus.ERROR
        )
        feed_ids, _ = Feed.wakeup_rsshub_feeds(limit=10, checked_before=run_start)
        assert feed.id not in feed_ids
