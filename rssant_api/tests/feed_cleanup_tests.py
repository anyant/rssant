from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from rssant_api.models import (
    DiscoverCategory,
    DiscoverFeed,
    Feed,
    FeedImportItem,
    FeedStatus,
    FeedUrlMap,
    UnionFeed,
    UserFeed,
)
from rssant_api.models.discover import DiscoverService


@pytest.mark.dbtest
class FeedCleanupTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser(
            'cleanup', email=None, password='test123456'
        )
        self.now = timezone.now()

    def _feed(self, url, status=FeedStatus.ERROR, synced_days=100, **kwargs):
        if synced_days is None:
            dt_synced = None
        else:
            dt_synced = self.now - timedelta(days=synced_days)
        feed = Feed(
            url=url,
            status=status,
            dt_updated=self.now,
            dt_checked=dt_synced,
            dt_synced=dt_synced,
            freeze_level=kwargs.pop('freeze_level', 240),
            **kwargs,
        )
        feed.save()
        return feed

    def _subscribe(self, feed):
        return UserFeed.objects.create(
            user=self.user, feed=feed, dt_updated=self.now
        )

    def test_archive_orphan_feeds(self):
        error_old = self._feed('https://example.com/error-old.xml')
        pending_old = self._feed(
            'https://example.com/pending-old.xml', status=FeedStatus.PENDING
        )
        rsshub_old = self._feed(
            'https://rsshub.app/v2ex/topics/hot', status=FeedStatus.ERROR
        )
        error_recent = self._feed(
            'https://example.com/error-recent.xml', synced_days=89
        )
        error_with_sub = self._feed('https://example.com/error-sub.xml')
        self._subscribe(error_with_sub)
        ready_old = self._feed(
            'https://example.com/ready-old.xml', status=FeedStatus.READY
        )
        category = DiscoverCategory.objects.create(slug='test', name='测试')
        discover_old = self._feed('https://example.com/discover-old.xml')
        DiscoverFeed.objects.create(feed=discover_old, category=category)

        assert Feed.orphan_feed_counts() == {
            FeedStatus.ERROR: 2,
            FeedStatus.PENDING: 1,
        }
        assert Feed.archive_orphan_feeds(limit=100) == 3
        for feed in (error_old, pending_old, rsshub_old):
            feed.refresh_from_db()
            assert feed.status == FeedStatus.DISCARD
            assert '已归档' in feed.warnings
        for feed in (error_recent, error_with_sub, ready_old, discover_old):
            feed.refresh_from_db()
            assert feed.status != FeedStatus.DISCARD
        assert Feed.archive_orphan_feeds(limit=100) == 0

    def test_revive_discard_with_subscribers(self):
        discard_sub = self._feed(
            'https://example.com/discard-sub.xml', status=FeedStatus.DISCARD
        )
        self._subscribe(discard_sub)
        discard_no_sub = self._feed(
            'https://example.com/discard-no-sub.xml', status=FeedStatus.DISCARD
        )

        assert Feed.count_discard_with_subscribers() == 1
        assert Feed.revive_discard_with_subscribers() == 1
        discard_sub.refresh_from_db()
        assert discard_sub.status == FeedStatus.PENDING
        assert discard_sub.dt_checked is None
        assert discard_sub.freeze_level == 1
        assert not discard_sub.warnings
        discard_no_sub.refresh_from_db()
        assert discard_no_sub.status == FeedStatus.DISCARD
        assert Feed.revive_discard_with_subscribers() == 0

    def test_create_by_url_revives_discard(self):
        feed = self._feed(
            'https://example.com/discard-subscribe.xml',
            status=FeedStatus.DISCARD,
        )
        FeedUrlMap.objects.create(
            source='https://example.com/original-url', target=feed.url
        )
        union_feed, creation = UnionFeed.create_by_url(
            user_id=self.user.id, url='https://example.com/original-url'
        )
        assert creation is None
        assert union_feed is not None
        feed.refresh_from_db()
        assert feed.status == FeedStatus.PENDING
        assert feed.dt_checked is None
        assert feed.freeze_level == 1
        assert UserFeed.objects.filter(user=self.user, feed=feed).exists()

    def test_create_by_imports_revives_discard(self):
        feed = self._feed(
            'https://example.com/discard-import.xml',
            status=FeedStatus.DISCARD,
        )
        FeedUrlMap.objects.create(source=feed.url, target=feed.url)
        result = UnionFeed.create_by_imports(
            user_id=self.user.id,
            imports=[FeedImportItem(url=feed.url, title=None, group=None)],
        )
        assert result.num_created_feeds == 1
        feed.refresh_from_db()
        assert feed.status == FeedStatus.PENDING
        assert feed.dt_checked is None
        assert feed.freeze_level == 1
        assert UserFeed.objects.filter(user=self.user, feed=feed).exists()

    def test_upsert_discover_item_revives_discard(self):
        feed = self._feed(
            'https://example.com/discard-discover.xml',
            status=FeedStatus.DISCARD,
        )
        DiscoverCategory.objects.create(slug='cleanup', name='清理')
        item = DiscoverService.upsert_item(
            url=feed.url, category_slug='cleanup'
        )
        feed.refresh_from_db()
        assert feed.status == FeedStatus.PENDING
        assert feed.dt_checked is None
        assert item.feed_id == feed.id
