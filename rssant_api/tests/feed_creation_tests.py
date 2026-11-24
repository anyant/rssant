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
