import pytest
from django.contrib.auth.models import User
from django.test import TransactionTestCase
from django.utils import timezone

from rssant_api.models import (
    STORY_SERVICE,
    Feed,
    FeedStatus,
    FeedUrlMap,
    Story,
    StoryInfo,
    UnionFeed,
    UserFeed,
)
from rssant_api.models.feed import (
    RETENTION_INACTIVE_FEED,
    RETENTION_READER_DAYS,
)
from rssant_config import CONFIG


def make_feed(url, total_storys=0, dt_checked=None):
    feed = Feed(
        title=url,
        url=url,
        status=FeedStatus.READY,
        total_storys=total_storys,
        dt_updated=timezone.now(),
        dt_checked=dt_checked,
    )
    feed.save()
    return feed


@pytest.mark.dbtest
class RetentionClassTestCase(TransactionTestCase):
    """按读者活性分档：活跃 300 / 有订阅没人读 30 / 无订阅 0"""

    def setUp(self):
        self.active_user = User.objects.create_user('active', password='test123456')
        self.inactive_user = User.objects.create_user('inactive', password='test123456')
        retention = CONFIG.feed_story_retention
        self.feed_active = make_feed('https://a.example.com/feed.xml', total_storys=retention + 10)
        self.feed_inactive = make_feed('https://b.example.com/feed.xml', total_storys=RETENTION_INACTIVE_FEED + 10)
        self.feed_nosub = make_feed('https://c.example.com/feed.xml', total_storys=10)
        UserFeed(user=self.active_user, feed=self.feed_active,
                 dt_updated=timezone.now()).save()
        UserFeed(user=self.inactive_user, feed=self.feed_inactive,
                 dt_updated=timezone.now() - timezone.timedelta(days=RETENTION_READER_DAYS + 10)).save()

    def test_take_retention_feeds_by_activity(self):
        feeds = Feed.take_retention_feeds(limit=10)
        got = {x['feed_id']: x['retention'] for x in feeds}
        self.assertEqual(got.get(self.feed_active.id), CONFIG.feed_story_retention)
        self.assertEqual(got.get(self.feed_inactive.id), RETENTION_INACTIVE_FEED)
        self.assertEqual(got.get(self.feed_nosub.id), 0)
        self.assertEqual(len(got), 3)

    def test_active_user_subscription_keeps_all(self):
        # 活跃用户订阅的源，即使这个源自己没被读过（userfeed.dt_updated 为空）也按 A 处理
        feed = make_feed(
            'https://d.example.com/feed.xml',
            total_storys=CONFIG.feed_story_retention + 10,
        )
        UserFeed(user=self.active_user, feed=feed, dt_updated=None).save()
        feeds = Feed.take_retention_feeds(limit=10)
        got = {x['feed_id']: x['retention'] for x in feeds}
        self.assertEqual(got.get(feed.id), CONFIG.feed_story_retention)

    def test_take_retention_feeds_skips_clean_feed(self):
        self.feed_nosub.retention_offset = self.feed_nosub.total_storys
        self.feed_nosub.save()
        feeds = Feed.take_retention_feeds(limit=10)
        ids = [x['feed_id'] for x in feeds]
        self.assertNotIn(self.feed_nosub.id, ids)


@pytest.mark.dbtest
class RetentionDeleteTestCase(TransactionTestCase):
    """删除走保留接口，用户收藏的文章副本不能丢"""

    def setUp(self):
        self.user = User.objects.create_user('reader', password='test123456')
        self.feed = make_feed('https://blog.example.com/feed.xml', dt_checked=timezone.now())
        UserFeed(user=self.user, feed=self.feed, dt_updated=timezone.now()).save()

    def _save_storys(self, count):
        now = timezone.datetime(2020, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        storys = []
        for i in range(count):
            dt = now + timezone.timedelta(hours=i)
            storys.append(dict(
                unique_id='story-{}'.format(i),
                title='story {}'.format(i),
                link='https://blog.example.com/{}'.format(i),
                content='content {}'.format(i),
                dt_published=dt,
                dt_updated=dt,
            ))
        STORY_SERVICE.bulk_save_by_feed(self.feed.id, storys, batch_size=10)

    def test_delete_by_retention_keeps_favorite_copy(self):
        self._save_storys(10)
        self.assertEqual(StoryInfo.objects.filter(
            pk__gte=self.feed.id << 32, pk__lt=(self.feed.id + 1) << 32).count(), 10)
        # 用户收藏第 2 篇，会在 story 表留下一份独立副本
        STORY_SERVICE.set_user_marked(self.feed.id, offset=2, is_user_marked=True)
        self.assertTrue(Story.objects.filter(
            feed_id=self.feed.id, offset=2, is_user_marked=True).exists())

        n = STORY_SERVICE.delete_by_retention(self.feed.id, retention=5, limit=100)
        self.assertGreater(n, 0)
        remain = StoryInfo.objects.filter(
            pk__gte=self.feed.id << 32, pk__lt=(self.feed.id + 1) << 32).count()
        self.assertEqual(remain, 5)
        story = Story.objects.filter(feed_id=self.feed.id, offset=2).first()
        self.assertIsNotNone(story, '收藏副本被删了')
        self.assertTrue(story.is_user_marked)
        self.assertEqual(story.content, 'content 2')

    def test_delete_by_retention_zero_removes_all(self):
        self._save_storys(10)
        STORY_SERVICE.delete_by_retention(self.feed.id, retention=0, limit=100)
        remain = StoryInfo.objects.filter(
            pk__gte=self.feed.id << 32, pk__lt=(self.feed.id + 1) << 32).count()
        self.assertEqual(remain, 0)
        self.feed.refresh_from_db()
        self.assertEqual(self.feed.retention_offset, self.feed.total_storys)


@pytest.mark.dbtest
class SubscribeRefreshTestCase(TransactionTestCase):
    """新订阅已有的源时，清空 dt_checked，让它立刻被抓一次"""

    def setUp(self):
        self.user = User.objects.create_user('subscriber', password='test123456')
        self.feed = make_feed('https://old.example.com/feed.xml', dt_checked=timezone.now())
        FeedUrlMap(source='https://old.example.com/', target=self.feed.url).save()

    def test_subscribe_resets_dt_checked(self):
        self.assertIsNotNone(Feed.get_by_pk(self.feed.id).dt_checked)
        UnionFeed.create_by_url(user_id=self.user.id, url='https://old.example.com/')
        self.assertIsNone(Feed.get_by_pk(self.feed.id).dt_checked)
