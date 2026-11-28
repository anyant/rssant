import json

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone
from rest_framework.authtoken.models import Token

from rssant_api.models import (
    STORY_SERVICE,
    Feed,
    FeedStatus,
    StoryInfo,
    UnionStory,
    UserFeed,
)
from rssant_api.models.story_info import StoryId
from rssant_common.unionid import encode


def make_feed(title, url, storys, dt_updated=None):
    feed = Feed(
        title=title,
        url=url,
        status=FeedStatus.READY,
        dt_updated=dt_updated or timezone.now(),
        dt_checked=timezone.now(),
    )
    feed.save()
    STORY_SERVICE.bulk_save_by_feed(feed.id, storys, batch_size=100)
    feed.refresh_from_db()
    return feed


def make_storys(url, count, now, hours_ago=1):
    storys = []
    for i in range(count):
        dt = now - timezone.timedelta(hours=hours_ago) + timezone.timedelta(minutes=i)
        storys.append(dict(
            unique_id=f'{url}/{i}',
            title=f'{url} story {i}',
            link=f'https://read.example.com/{url}/{i}',
            content=f'<p>content {url} {i}</p>',
            summary=f'summary {url} {i}',
            dt_published=dt,
            dt_updated=dt,
        ))
    return storys


class StoryRecentTestCase(TestCase):
    def tearDown(self):
        keys = []
        for feed_id in self.feed_ids:
            keys.extend((feed_id, offset) for offset in range(10))
        STORY_SERVICE._storage.batch_delete_content(keys)

    def setUp(self):
        self.now = timezone.now()
        self.feed_ids = []
        self.user = User.objects.create_user('recent', password='test123456')
        token = Token.objects.create(user=self.user)
        self.client.defaults['HTTP_AUTHORIZATION'] = 'Token ' + token.key
        self.feed = make_feed(
            '测试订阅', 'https://recent.example.com/feed.xml',
            make_storys('recent', 5, self.now),
        )
        self.feed_ids.append(self.feed.id)
        self.user_feed = UserFeed.objects.create(
            user=self.user, feed=self.feed, story_offset=0,
        )

    def _post(self, url, data):
        return self.client.post(
            url, data=json.dumps(data), content_type='application/json',
        )

    def _recent(self, **kwargs):
        data = {'days': 1, 'limit': 100, 'per_feed_limit': 10}
        data.update(kwargs)
        return self._post('/api/v1/story.query_recent', data).json()

    def test_days_filter_uses_created_time(self):
        old = self.now - timezone.timedelta(days=2)
        for offset in (0, 1):
            StoryInfo.objects.filter(
                id=StoryId.encode(self.feed.id, offset)
            ).update(dt_created=old)
        data = self._recent(days=1)
        assert [x['offset'] for x in data['storys']] == [4, 3, 2]
        data = self._recent(days=3)
        assert len(data['storys']) == 5

    def test_unread_only_uses_story_offset_and_single_read(self):
        UserFeed.objects.filter(pk=self.user_feed.pk).update(story_offset=2)
        UnionStory.set_read_by_feed_offset(
            (self.user.id, self.feed.id), 3, is_read=True,
        )
        data = self._recent(unread_only=True)
        assert [x['offset'] for x in data['storys']] == [4, 2]
        data = self._recent(unread_only=False)
        by_offset = {x['offset']: x['is_read'] for x in data['storys']}
        assert by_offset == {0: True, 1: True, 2: False, 3: True, 4: False}

    def test_per_feed_limit(self):
        feed2 = make_feed(
            '第二个订阅', 'https://recent2.example.com/feed.xml',
            make_storys('recent2', 5, self.now),
        )
        self.feed_ids.append(feed2.id)
        UserFeed.objects.create(user=self.user, feed=feed2, story_offset=0)
        data = self._recent(unread_only=True, per_feed_limit=2)
        assert data['total'] == 4
        feeds = {}
        for x in data['storys']:
            feeds.setdefault(x['feed']['id'], []).append(x)
        assert [len(v) for v in feeds.values()] == [2, 2]
        assert all('title' in x['feed'] for x in data['storys'])

    def test_is_read_effective_on_get_and_list(self):
        UserFeed.objects.filter(pk=self.user_feed.pk).update(story_offset=3)
        data = self._post('/api/v1/story.get', {
            'feed_id': encode(self.user.id, self.feed.id),
            'offset': 0, 'detail': False,
        }).json()
        assert data['is_read'] is True
        data = self._post('/api/v1/story.query', {
            'feed_id': encode(self.user.id, self.feed.id),
            'offset': 2, 'size': 2, 'detail': False,
        }).json()
        assert [(x['offset'], x['is_read']) for x in data['storys']] == [
            (2, True), (3, False),
        ]

    def test_set_read_false_before_offset_stays_read(self):
        UserFeed.objects.filter(pk=self.user_feed.pk).update(story_offset=3)
        data = self._post('/api/v1/story.set_read', {
            'feed_id': encode(self.user.id, self.feed.id),
            'offset': 0, 'is_read': False,
        }).json()
        assert data['is_read'] is True

    def test_query_batch_content(self):
        keys = [{
            'feed_id': encode(self.user.id, self.feed.id),
            'offset': 4, 'limit': 1,
        }]
        data = self._post('/api/v1/story.query_batch', {
            'storys': keys, 'detail': True, 'content': True,
            'content_format': 'markdown',
        }).json()
        content = data['storys'][0]['content']
        assert 'content recent 4' in content
        assert '<p>' not in content
        data = self._post('/api/v1/story.query_batch', {
            'storys': keys, 'detail': True, 'content': False,
        }).json()
        assert not data['storys'][0].get('content')

    def test_list_detail_true_has_no_content(self):
        data = self._post('/api/v1/story.query', {
            'feed_id': encode(self.user.id, self.feed.id),
            'offset': 0, 'size': 5, 'detail': True,
        }).json()
        assert all(not x.get('content') for x in data['storys'])
        data = self._recent(detail=True)
        assert all(not x.get('content') for x in data['storys'])

    def test_daily_digest_doc_is_public(self):
        response = self.client.get('/docs/daily-digest.md')
        assert response.status_code == 200
        assert 'text/markdown' in response['Content-Type']
        content = response.content.decode('utf-8')
        assert '你的任务' in content
        assert 'story.query_recent' in content
        assert 'httpx' in content
