import json
from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from rssant_api.models import (
    Feed,
    FeedCreation,
    FeedImportItem,
    FeedStatus,
    UnionFeed,
    union_feed,
)

FEED_CREATION_RETENTION_SECONDS = 24 * 60 * 60


@pytest.mark.dbtest
class FeedCreationSoftDeleteTestCase(TestCase):
    """订阅创建记录超过保留时间后软删除，数据永久保留"""

    def setUp(self):
        self.user = User.objects.create_user(
            'creator', password='test123456'
        )

    def _make_creation(self, url, *, status=FeedStatus.PENDING,
                       hours_ago=0, message='', **kwargs):
        creation = FeedCreation(
            user=self.user, url=url, status=status, message=message,
            **kwargs,
        )
        creation.save()
        if hours_ago:
            dt_created = timezone.now() - timezone.timedelta(hours=hours_ago)
            FeedCreation.objects.filter(pk=creation.pk).update(
                dt_created=dt_created
            )
            creation.refresh_from_db()
        return creation

    def _soft_delete_expired(self):
        return FeedCreation.soft_delete_by_status(
            survival_seconds=FEED_CREATION_RETENTION_SECONDS
        )

    def test_old_creation_soft_deleted_and_hidden(self):
        from rssant_harbor.harbor_service import HARBOR_SERVICE

        old = self._make_creation(
            'https://old.example.com/feed.xml',
            hours_ago=25,
            message='连接超时',
        )
        new = self._make_creation('https://new.example.com/feed.xml')

        result = HARBOR_SERVICE.clean_feed_creation()
        self.assertEqual(result['num_deleted'], 1)

        old.refresh_from_db()
        self.assertIsNotNone(old.deleted_at)
        self.assertEqual(old.url, 'https://old.example.com/feed.xml')
        self.assertEqual(old.message, '连接超时')

        ids = [x.id for x in FeedCreation.query_by_user(self.user.id)]
        self.assertEqual(ids, [new.id])
        with self.assertRaises(FeedCreation.DoesNotExist):
            FeedCreation.get_by_pk(old.id, user_id=self.user.id)
        self.assertEqual(
            FeedCreation.get_by_pk(new.id, user_id=self.user.id).id,
            new.id,
        )

    def test_soft_delete_is_idempotent(self):
        self._make_creation(
            'https://old.example.com/feed.xml', hours_ago=25
        )
        first = self._soft_delete_expired()
        second = self._soft_delete_expired()
        self.assertEqual(first, 1)
        self.assertEqual(second, 0)

    def test_retry_query_excludes_soft_deleted(self):
        active = self._make_creation(
            'https://active.example.com/feed.xml', hours_ago=5
        )
        self._make_creation(
            'https://expired.example.com/feed.xml', hours_ago=25
        )
        self._soft_delete_expired()
        id_urls = FeedCreation.query_id_urls_by_status(
            FeedStatus.PENDING, survival_seconds=4 * 60 * 60
        )
        self.assertEqual(id_urls, [(active.id, active.url)])

    def test_bulk_set_pending_skips_soft_deleted(self):
        creation = self._make_creation(
            'https://old.example.com/feed.xml',
            status=FeedStatus.ERROR,
            hours_ago=25,
        )
        self._soft_delete_expired()
        num_updated = FeedCreation.bulk_set_pending([creation.id])
        self.assertEqual(num_updated, 0)
        creation.refresh_from_db()
        self.assertEqual(creation.status, FeedStatus.ERROR)

    def test_capacity_ignores_soft_deleted(self):
        for i in range(2):
            self._make_creation(
                'https://old-{}.example.com/feed.xml'.format(i),
                hours_ago=25,
            )
        self._soft_delete_expired()
        imports = [
            FeedImportItem(
                url='https://new.example.com/feed.xml',
                title=None,
                group=None,
            )
        ]
        with patch.object(union_feed, 'MAX_FEED_COUNT', 2):
            result = UnionFeed.create_by_imports(
                user_id=self.user.id, imports=imports
            )
        self.assertEqual(result.num_feed_creations, 1)

    def test_delete_feed_keeps_creation_but_hides_from_user(self):
        feed = Feed(
            title='test feed',
            url='https://blog.example.com/feed.xml',
            status=FeedStatus.READY,
            dt_updated=timezone.now(),
        )
        feed.save()
        creation = self._make_creation(feed.url, feed=feed)
        Feed.objects.filter(pk=feed.pk).delete()
        creation.refresh_from_db()
        self.assertIsNone(creation.feed_id)
        self.assertIsNotNone(creation.deleted_at)
        # 数据保留，但用户侧立即不可见，和删除 Feed 前一致
        self.assertTrue(
            FeedCreation.objects.filter(pk=creation.pk).exists()
        )
        self.assertEqual(FeedCreation.query_by_user(self.user.id), [])
        with self.assertRaises(FeedCreation.DoesNotExist):
            FeedCreation.get_by_pk(creation.id, user_id=self.user.id)


@pytest.mark.dbtest
class FeedCreationDetailTestCase(TestCase):
    """失败描述只随 detail=true 的详情接口返回"""

    def setUp(self):
        self.user = User.objects.create_user('detail', password='test123456')
        self.error_message = (
            '这个地址不存在。检查链接是否写错，或换一个 RSS 地址试试。'
        )
        self.creation = FeedCreation(
            user=self.user,
            url='https://example.com/missing',
            status=FeedStatus.ERROR,
            message='查找日志',
            error_message=self.error_message,
        )
        self.creation.save()
        assert self.client.login(username='detail', password='test123456')

    def _post(self, api, data):
        return self.client.post(
            '/api/v1/' + api,
            data=json.dumps(data),
            content_type='application/json',
        )

    def test_get_creation_detail_returns_error_message(self):
        response = self._post(
            'feed.get_creation', {'id': self.creation.id, 'detail': True}
        )
        assert response.status_code == 200, response.content
        data = response.json()
        assert data['message'] == '查找日志'
        assert data['error_message'] == self.error_message

    def test_get_creation_without_detail_has_empty_error_message(self):
        response = self._post(
            'feed.get_creation', {'id': self.creation.id, 'detail': False}
        )
        assert response.status_code == 200, response.content
        # 输出 schema 会把 optional 字符串补成空串，和 message 一致；不含失败描述
        assert response.json()['error_message'] == ''

    def test_query_creation_has_empty_error_message(self):
        response = self._post(
            'feed.query_creation', {'limit': 100, 'detail': True}
        )
        assert response.status_code == 200, response.content
        items = response.json()['feed_creations']
        assert len(items) == 1
        assert items[0]['error_message'] == ''
