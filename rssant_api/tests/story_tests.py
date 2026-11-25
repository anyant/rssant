import json

import pytest
from django.contrib.auth.models import User
from django.utils import timezone
from django.test import TestCase, TransactionTestCase
from validr import T

from rssant_common.unionid import encode
from rssant_common.validator import compiler
from rssant_api.models import Feed, FeedStatus, UserFeed
from rssant_api.models import STORY_SERVICE, Story, StoryInfo
from rssant.helper.content_hash import compute_hash_base64


StorySchema = T.dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str.optional,
    author=T.str.optional,
    link=T.str.optional,
    image_url=T.url.optional,
    iframe_url=T.url.optional,
    audio_url=T.url.optional,
    enclosure_urls=T.list(T.str).optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

validate_story = compiler.compile(StorySchema)


@pytest.mark.dbtest
class StoryTestCase(TransactionTestCase):

    def setUp(self):
        print('setUp')
        storys = []
        updated_storys = []
        now = timezone.datetime(2020, 6, 1, 12, 12, 12, tzinfo=timezone.utc)
        for i in range(200):
            dt = now + timezone.timedelta(minutes=i)
            content = f'test story content {i}' * (i % 5)
            content_hash_base64 = compute_hash_base64(content)
            summary = content[:30]
            story = {
                'unique_id': f'blog.example.com/{i}',
                'title': f'test story {i}',
                'content_hash_base64': content_hash_base64,
                'author': 'tester',
                'link': f'https://blog.example.com/{i}.html',
                'dt_published': dt,
                'dt_updated': dt,
                'summary': summary,
                'content': content,
            }
            storys.append(validate_story(story))
            updated_story = dict(story)
            updated_content = f'test story content updated {i}' * (i % 5 + 1)
            updated_story.update(
                content=updated_content,
                content_hash_base64=compute_hash_base64(updated_content),
            )
            updated_storys.append(validate_story(updated_story))
        self.storys = storys
        self.updated_storys = updated_storys

        feed = Feed(
            title='test feed',
            url='https://blog.example.com/feed.xml',
            status=FeedStatus.READY,
            dt_updated=timezone.now(),
        )
        feed.save()
        self.feed_id = feed.id

    def assert_feed_total_storys(self, expect):
        total_storys = Feed.get_by_pk(self.feed_id).total_storys
        self.assertEqual(total_storys, expect)

    def assert_total_story_infos(self, expect):
        total_storys = StoryInfo.objects.count()
        self.assertEqual(total_storys, expect)

    def test_new_bulk_save_by_feed(self):
        storys_0_30 = self.storys[:30]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)
        self.assert_feed_total_storys(30)
        self.assert_total_story_infos(30)

        storys_20_50 = self.storys[20:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_20_50, batch_size=10)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(50)

        updated_storys_30_50 = self.updated_storys[30:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, updated_storys_30_50, batch_size=10)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(50)

    def test_mix_bulk_save_by_feed(self):
        storys_0_30 = self.storys[:30]
        modified = Story.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)
        self.assert_feed_total_storys(30)
        self.assert_total_story_infos(0)

        storys_10_50 = self.updated_storys[10:30] + self.storys[30:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_10_50, batch_size=10)
        self.assertEqual(len(modified), 40)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(40)

        storys_40_60 = self.storys[40:60]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_40_60, batch_size=10)
        self.assertEqual(len(modified), 10)
        self.assert_feed_total_storys(60)
        self.assert_total_story_infos(50)

    def test_bulk_save_by_feed_refresh(self):
        storys_0_20 = self.storys[:20]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(20)
        self.assert_total_story_infos(20)

        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 0)
        self.assert_feed_total_storys(20)
        self.assert_total_story_infos(20)

        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10, is_refresh=True)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(20)
        self.assert_total_story_infos(20)

    def test_update_story(self):
        storys_0_20 = self.storys[:20]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(20)
        self.assert_total_story_infos(20)

        story_10 = self.updated_storys[10]
        data = {k: story_10[k] for k in ['content', 'summary', 'dt_published']}
        STORY_SERVICE.update_story(self.feed_id, 10, data)
        content_data = {'content': data['content']}
        STORY_SERVICE.update_story(self.feed_id, 10, content_data)

    def test_delete_by_retention(self):
        storys_0_30 = self.storys[:30]
        modified = Story.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)
        self.assert_feed_total_storys(30)
        self.assert_total_story_infos(0)

        storys_20_50 = self.storys[20:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_20_50, batch_size=10)
        self.assertEqual(len(modified), 20)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(20)

        n = STORY_SERVICE.delete_by_retention(self.feed_id, retention=10, limit=10)
        self.assertEqual(n, 10)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(20)

        n = STORY_SERVICE.delete_by_retention(self.feed_id, retention=10, limit=50)
        self.assertEqual(n, 30)
        self.assert_feed_total_storys(50)
        self.assert_total_story_infos(10)

    def test_story_dt_and_content_length(self):
        dt = timezone.datetime(2019, 6, 1, 12, 12, 12, tzinfo=timezone.utc)
        story = {
            'unique_id': 'blog.example.com/1',
            'title': 'test story 1',
            'dt_published': dt,
            'dt_updated': dt,
        }
        modified = Story.bulk_save_by_feed(
            self.feed_id, [validate_story(story)], batch_size=10)
        self.assertEqual(len(modified), 1)
        self.assert_feed_total_storys(1)
        self.assert_total_story_infos(0)
        dt_created = modified[0].dt_created
        dt_published = modified[0].dt_published
        assert modified[0].dt_updated == dt

        dt = dt + timezone.timedelta(days=1)
        updated_content = 'updated_content 1'
        story.update(
            content=updated_content,
            content_hash_base64=compute_hash_base64(updated_content),
            dt_published=dt,
            dt_updated=dt,
        )
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, [validate_story(story)], batch_size=10)
        self.assertEqual(len(modified), 1)
        self.assert_feed_total_storys(1)
        self.assert_total_story_infos(1)
        assert modified[0].dt_created == dt_created
        assert modified[0].dt_published == dt_published
        assert modified[0].dt_updated == dt
        assert modified[0].content_length == len(updated_content)

        dt = dt + timezone.timedelta(days=2)
        updated_content = 'updated_content 22'
        story.update(
            content=updated_content,
            content_hash_base64=compute_hash_base64(updated_content),
            dt_published=dt,
            dt_updated=dt,
        )
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, [validate_story(story)], batch_size=10)
        self.assertEqual(len(modified), 1)
        self.assert_feed_total_storys(1)
        self.assert_total_story_infos(1)
        assert modified[0].dt_created == dt_created
        assert modified[0].dt_published == dt_published
        assert modified[0].dt_updated == dt
        assert modified[0].content_length == len(updated_content)


STORY_OFFSET_APIS = [
    '/api/v1/story.get',
    '/api/v1/story.set_watched',
    '/api/v1/story.set_favorited',
]


@pytest.mark.dbtest
class StoryOffsetRequiredTestCase(TestCase):
    """story.get/set_watched/set_favorited 必须带 offset，缺失返回 400 而不是 500"""

    def setUp(self):
        self.user = User.objects.create_user('offset', password='test123456')
        assert self.client.login(username='offset', password='test123456')
        feed = Feed(
            title='test feed',
            url='https://offset.example.com/feed.xml',
            status=FeedStatus.READY,
            total_storys=1,
            dt_updated=timezone.now(),
        )
        feed.save()
        UserFeed(user_id=self.user.id, feed_id=feed.id, title=feed.title).save()
        Story(
            feed_id=feed.id,
            offset=0,
            unique_id='offset/0',
            title='test story',
            link='https://offset.example.com/0.html',
            dt_published=timezone.now(),
            content='test content',
        ).save()
        self.feed_id = encode(self.user.id, feed.id)

    def post(self, url, data):
        return self.client.post(
            url, data=json.dumps(data), content_type='application/json'
        )

    def test_missing_offset(self):
        for url in STORY_OFFSET_APIS:
            response = self.post(url, {'feed_id': self.feed_id})
            assert response.status_code == 400, url
            assert response.json()['field'] == 'offset', url

    def test_has_offset(self):
        for url in STORY_OFFSET_APIS:
            response = self.post(url, {'feed_id': self.feed_id, 'offset': 0})
            assert response.status_code == 200, url


@pytest.mark.dbtest
class StoryEnclosureTestCase(TestCase):
    """enclosure_urls 只在详情接口返回，列表接口保持原样"""

    def setUp(self):
        self.user = User.objects.create_user('enclosure', password='test123456')
        assert self.client.login(username='enclosure', password='test123456')
        now = timezone.now()
        feed = Feed(
            title='enclosure feed',
            url='https://enclosure.example.com/feed.xml',
            status=FeedStatus.READY,
            dt_updated=now,
        )
        feed.save()
        UserFeed(user_id=self.user.id, feed_id=feed.id, title=feed.title).save()
        self.feed_id = feed.id
        self.enclosure_urls = [
            'https://enclosure.example.com/report.pdf',
            'magnet:?xt=urn:btih:ABCDEF&dn=example',
        ]
        storys = []
        for offset, urls in enumerate([self.enclosure_urls, None]):
            content = f'<p>story {offset}</p>'
            story = {
                'unique_id': f'enclosure/{offset}',
                'title': f'story {offset}',
                'content_hash_base64': compute_hash_base64(content),
                'link': f'https://enclosure.example.com/{offset}.html',
                'dt_published': now,
                'dt_updated': now,
                'summary': content,
                'content': content,
                'enclosure_urls': urls,
            }
            storys.append(validate_story(story))
        STORY_SERVICE.bulk_save_by_feed(feed.id, storys, batch_size=10)

    def post(self, url, data):
        return self.client.post(
            url, data=json.dumps(data), content_type='application/json'
        )

    def test_story_info_enclosure_urls(self):
        story_info = StoryInfo.get(self.feed_id, 0, detail=True)
        assert story_info.enclosure_urls == self.enclosure_urls
        old_story_info = StoryInfo.get(self.feed_id, 1, detail=True)
        assert old_story_info.enclosure_urls is None

    def test_story_get_detail_has_enclosure_urls(self):
        response = self.post('/api/v1/story.get', {
            'feed_id': encode(self.user.id, self.feed_id),
            'offset': 0,
            'detail': True,
        })
        assert response.status_code == 200
        assert response.json()['enclosure_urls'] == self.enclosure_urls

    def test_story_get_no_detail_without_enclosure_urls(self):
        response = self.post('/api/v1/story.get', {
            'feed_id': encode(self.user.id, self.feed_id),
            'offset': 0,
            'detail': False,
        })
        assert response.status_code == 200
        assert 'enclosure_urls' not in response.json()

    def test_story_query_no_detail_without_enclosure_urls(self):
        response = self.post('/api/v1/story.query', {
            'feed_id': encode(self.user.id, self.feed_id),
            'offset': 0,
            'size': 10,
            'detail': False,
        })
        assert response.status_code == 200
        storys = response.json()['storys']
        assert len(storys) == 2
        assert 'enclosure_urls' not in storys[0]
        assert 'enclosure_urls' not in storys[1]

    def test_story_query_detail_has_enclosure_urls(self):
        response = self.post('/api/v1/story.query', {
            'feed_id': encode(self.user.id, self.feed_id),
            'offset': 0,
            'size': 10,
            'detail': True,
        })
        assert response.status_code == 200
        storys = response.json()['storys']
        assert storys[0]['enclosure_urls'] == self.enclosure_urls
        assert 'enclosure_urls' not in storys[1]
