import io
import json
import zipfile

import pytest
from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import TestCase
from django.utils import timezone
from rest_framework.authtoken.models import Token

from rssant_api.models import Feed, FeedStatus, Story, UserFeed
from rssant_common.unionid import encode
from rssant_config import CONFIG


def make_feed(url, total_storys=0, retention_offset=0):
    feed = Feed(
        title='测试订阅',
        url=url,
        status=FeedStatus.READY,
        total_storys=total_storys,
        retention_offset=retention_offset,
        dt_updated=timezone.now(),
    )
    feed.save()
    return feed


def make_story(feed, offset, content='正文内容'):
    story = Story(
        feed_id=feed.id,
        offset=offset,
        unique_id='offset/{}'.format(offset),
        title='测试文章 {}'.format(offset),
        link='https://example.com/{}.html'.format(offset),
        dt_published=timezone.now(),
        content=content,
    )
    story.save()
    return story


def read_zip(response):
    body = b''.join(response.streaming_content)
    return zipfile.ZipFile(io.BytesIO(body)), body


@pytest.mark.dbtest
class ExportTestCase(TestCase):
    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user('exporter', password='test123456')
        self.token = Token.objects.create(user=self.user).key
        self.client.defaults['HTTP_AUTHORIZATION'] = 'Token ' + self.token
        self.feed = make_feed('https://export.example.com/feed.xml', total_storys=10)
        UserFeed(user=self.user, feed=self.feed, dt_updated=timezone.now()).save()

    def _favorite(self, story, dt_favorited):
        from rssant_api.models import UserStory

        UserStory(
            user_id=self.user.id,
            feed_id=story.feed_id,
            user_feed_id=UserFeed.objects.get(user=self.user, feed=self.feed).id,
            story_id=story.id,
            offset=story.offset,
            is_favorited=True,
            dt_favorited=dt_favorited,
        ).save()

    def test_favorites_export_structure(self):
        story1 = make_story(self.feed, 1, content='<p>Hello <b>world</b></p>')
        story2 = make_story(self.feed, 2, content='')
        story2.summary = '只有摘要'
        story2.save()
        self._favorite(story1, timezone.now())
        self._favorite(story2, timezone.now())

        response = self.client.get('/api/v1/story/export/favorited')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/zip')
        zf, body = read_zip(response)
        names = zf.namelist()
        self.assertIn('articles.json', names)
        self.assertIn('index.xlsx', names)
        self.assertIn('manifest.json', names)
        articles = json.loads(zf.read('articles.json'))
        manifest = json.loads(zf.read('manifest.json'))
        self.assertEqual(len(articles), 2)
        self.assertEqual(manifest['count'], 2)
        self.assertEqual(manifest['content_missing'], 1)
        self.assertEqual(len([x for x in names if x.startswith('content/')]), 2)
        article1 = [x for x in articles if not x['content_missing']][0]
        content_file = article1['content_file']
        self.assertIn('Hello **world**', zf.read(content_file).decode('utf-8'))
        article2 = [x for x in articles if x['content_missing']][0]
        self.assertIn('只有摘要', zf.read(article2['content_file']).decode('utf-8'))
        self.assertGreater(len(body), 0)

    def test_feed_export_uses_retention_window(self):
        for offset in range(10):
            make_story(self.feed, offset)
        old_retention = CONFIG.feed_story_retention
        CONFIG.feed_story_retention = 3
        try:
            feed_id = encode(self.user.id, self.feed.id)
            response = self.client.get('/api/v1/feed/{}/export/stories'.format(feed_id))
        finally:
            CONFIG.feed_story_retention = old_retention
        self.assertEqual(response.status_code, 200)
        zf, _ = read_zip(response)
        articles = json.loads(zf.read('articles.json'))
        feed_data = json.loads(zf.read('feed.json'))
        self.assertEqual(len(articles), 3)
        self.assertEqual([x['title'] for x in articles], ['测试文章 7', '测试文章 8', '测试文章 9'])
        self.assertEqual(feed_data['exported_count'], 3)

    def test_export_cooldown(self):
        make_story(self.feed, 1)
        first = self.client.get('/api/v1/feed/{}/export/stories'.format(
            encode(self.user.id, self.feed.id)))
        second = self.client.get('/api/v1/feed/{}/export/stories'.format(
            encode(self.user.id, self.feed.id)))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)

    def test_export_requires_login(self):
        client = self.client_class()
        response = client.get('/api/v1/story/export/favorited')
        self.assertIn(response.status_code, (401, 403))

    def test_feed_export_only_own_feed(self):
        other = User.objects.create_user('other', password='test123456')
        feed_id = encode(other.id, self.feed.id)
        response = self.client.get('/api/v1/feed/{}/export/stories'.format(feed_id))
        self.assertIn(response.status_code, (400, 403, 404))


@pytest.mark.dbtest
class TokenAndAgentDocTestCase(TestCase):
    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user('tokenuser', password='test123456')
        self.old_token = Token.objects.create(user=self.user).key
        self.client.defaults['HTTP_AUTHORIZATION'] = 'Token ' + self.old_token

    def test_reset_token_rotates_and_invalidates_old(self):
        response = self.client.post('/api/v1/user/reset_token/')
        self.assertEqual(response.status_code, 200)
        new_token = response.json()['token']
        self.assertNotEqual(new_token, self.old_token)

        old_client = self.client_class()
        old_client.defaults['HTTP_AUTHORIZATION'] = 'Token ' + self.old_token
        old_response = old_client.post(
            '/api/v1/feed.query', data=json.dumps({'detail': False}),
            content_type='application/json')
        self.assertIn(old_response.status_code, (401, 403))

        new_client = self.client_class()
        new_client.defaults['HTTP_AUTHORIZATION'] = 'Token ' + new_token
        new_response = new_client.post(
            '/api/v1/feed.query', data=json.dumps({'detail': False}),
            content_type='application/json')
        self.assertEqual(new_response.status_code, 200)

    def test_reset_token_requires_login(self):
        client = self.client_class()
        response = client.post('/api/v1/user/reset_token/')
        self.assertIn(response.status_code, (401, 403))

    def test_agent_doc_is_public(self):
        client = self.client_class()
        response = client.get('/docs/agent.md')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/markdown', response['Content-Type'])
        content = response.content.decode('utf-8')
        self.assertIn('## 发现订阅源', content)
        self.assertIn('## 添加订阅', content)
        self.assertIn('Authorization: Token <令牌>', content)
        self.assertIn('http://testserver', content)
