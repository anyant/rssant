import json

import pytest
from django.contrib.auth.models import User
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from rssant_api.models import (
    STORY_SERVICE,
    Feed,
    FeedStatus,
    Story,
    UnionFeed,
    UnionStory,
    UserFeed,
    UserStory,
)
from rssant_api.models.errors import StoryNotFoundError
from rssant_common.unionid import encode

STORY_COUNT = 5


def make_feed(url, total=STORY_COUNT):
    feed = Feed(
        title=url,
        url=url,
        status=FeedStatus.READY,
        dt_updated=timezone.now(),
        dt_checked=timezone.now(),
    )
    feed.save()
    now = timezone.datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    storys = []
    for i in range(total):
        dt = now + timezone.timedelta(hours=i)
        storys.append(dict(
            unique_id=f'{url}/{i}',
            title=f'story {i}',
            link=f'https://read.example.com/{i}',
            content=f'read story content {i}',
            dt_published=dt,
            dt_updated=dt,
            summary=f'read story summary {i}',
        ))
    STORY_SERVICE.bulk_save_by_feed(feed.id, storys, batch_size=100)
    # 大部分文章只有 StoryInfo，这里也建一份 Story 记录方便断言
    Story.objects.bulk_create([
        Story(
            feed_id=feed.id,
            offset=i,
            unique_id=f'{url}/{i}',
            title=f'story {i}',
            link=f'https://read.example.com/{i}',
            content=f'read story content {i}',
            dt_published=now + timezone.timedelta(hours=i),
            dt_updated=now + timezone.timedelta(hours=i),
            summary=f'read story summary {i}',
        )
        for i in range(total)
    ], batch_size=100)
    feed.refresh_from_db()
    return feed


@pytest.mark.dbtest
class StoryReadTestCase(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user('reader', password='test123456')
        self.feed = make_feed('https://read.example.com/feed.xml')
        UserFeed(user=self.user, feed=self.feed, story_offset=0).save()

    def _set_read(self, offset, is_read=True):
        return UnionStory.set_read_by_feed_offset(
            (self.user.id, self.feed.id), offset, is_read=is_read)

    def test_set_read_does_not_touch_watched(self):
        story = self._set_read(4)
        assert story.is_read is True
        assert story.is_watched is False
        user_story = UserStory.objects.get(
            user_id=self.user.id, feed_id=self.feed.id, offset=4)
        assert user_story.is_read is True
        assert user_story.is_watched is False
        db_story = Story.objects.get(feed_id=self.feed.id, offset=4)
        assert db_story.is_user_marked is False

    def test_num_unread_storys(self):
        union_feed = UnionFeed.get_by_id((self.user.id, self.feed.id))
        assert union_feed.num_unread_storys == STORY_COUNT
        self._set_read(4)
        union_feed = UnionFeed.get_by_id((self.user.id, self.feed.id))
        assert union_feed.num_unread_storys == STORY_COUNT - 1
        self._set_read(4, is_read=False)
        union_feed = UnionFeed.get_by_id((self.user.id, self.feed.id))
        assert union_feed.num_unread_storys == STORY_COUNT

    def test_set_read_does_not_change_story_offset(self):
        self._set_read(3)
        user_feed = UserFeed.objects.get(
            user_id=self.user.id, feed_id=self.feed.id)
        assert user_feed.story_offset == 0

    def test_watched_still_works(self):
        story = UnionStory.set_watched_by_feed_offset(
            (self.user.id, self.feed.id), 2, is_watched=True)
        assert story.is_watched is True
        user_story = UserStory.objects.get(
            user_id=self.user.id, feed_id=self.feed.id, offset=2)
        assert user_story.is_read is False
        assert user_story.is_watched is True


@pytest.mark.dbtest
class SetReadWithoutStoryTestCase(TransactionTestCase):
    """文章只有 StoryInfo、没有 Story 记录时（生产上的常见情况）也能标为已读"""

    def setUp(self):
        self.user = User.objects.create_user('info-only', password='test123456')
        self.feed = make_feed('https://info-only.example.com/feed.xml')
        Story.objects.filter(feed_id=self.feed.id).delete()
        UserFeed(user=self.user, feed=self.feed, story_offset=0).save()

    def test_set_read_creates_story_without_user_marked(self):
        assert not Story.objects.filter(feed_id=self.feed.id).exists()
        story = UnionStory.set_read_by_feed_offset(
            (self.user.id, self.feed.id), 3)
        assert story.is_read is True
        db_story = Story.objects.get(feed_id=self.feed.id, offset=3)
        assert db_story.is_user_marked is False
        user_story = UserStory.objects.get(
            user_id=self.user.id, feed_id=self.feed.id, offset=3)
        assert user_story.is_read is True

    def test_set_read_story_not_found(self):
        with pytest.raises(StoryNotFoundError):
            UnionStory.set_read_by_feed_offset(
                (self.user.id, self.feed.id), STORY_COUNT + 1)


@pytest.mark.dbtest
class QueryUnreadBatchTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user('unread', password='test123456')
        assert self.client.login(username='unread', password='test123456')
        self.feed = make_feed('https://unread.example.com/feed.xml', 200)
        UserFeed(user=self.user, feed=self.feed, story_offset=0).save()
        self.feed_unionid = encode(self.user.id, self.feed.id)

    def _post(self, items):
        return self.client.post(
            '/api/v1/story.query_unread_batch',
            data=json.dumps({'storys': items}),
            content_type='application/json',
        )

    def _mark_read(self, offsets):
        for offset in offsets:
            UnionStory.set_read_by_feed_offset(
                (self.user.id, self.feed.id), offset, is_read=True)

    def _offsets(self, response):
        return [x['offset'] for x in response.json()['storys']]

    def test_skip_many_read(self):
        # 最新 150 篇都读过，仍能找到更早的未读
        self._mark_read(range(50, 200))
        response = self._post([{'feed_id': self.feed_unionid, 'limit': 3}])
        assert self._offsets(response) == [49, 48, 47]

    def test_no_unread(self):
        self._mark_read(range(0, 200))
        response = self._post([{'feed_id': self.feed_unionid, 'limit': 3}])
        assert self._offsets(response) == []

    def test_limit(self):
        response = self._post([{'feed_id': self.feed_unionid, 'limit': 2}])
        assert self._offsets(response) == [199, 198]

    def test_story_offset_boundary(self):
        UserFeed.objects.filter(
            user_id=self.user.id, feed_id=self.feed.id
        ).update(story_offset=100)
        self._mark_read([199])
        response = self._post([{'feed_id': self.feed_unionid, 'limit': 3}])
        assert self._offsets(response) == [198, 197, 196]

    def test_cross_feed(self):
        feed2 = make_feed('https://unread2.example.com/feed.xml', 30)
        UserFeed(user=self.user, feed=feed2, story_offset=0).save()
        feed2_unionid = encode(self.user.id, feed2.id)
        response = self._post([
            {'feed_id': self.feed_unionid, 'limit': 2},
            {'feed_id': feed2_unionid, 'limit': 2},
        ])
        assert len(self._offsets(response)) == 4
