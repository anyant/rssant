from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from rest_framework.authtoken.models import Token

from rssant_api.models import Feed, FeedStatus, UserFeed
from rssant_api.models.discover import (
    DISCOVER_CACHE_TTL,
    DiscoverFeed,
    DiscoverService,
)


def _token(user):
    return Token.objects.get_or_create(user=user)[0].key


@pytest.mark.dbtest
class DiscoverTestCase(TestCase):
    def setUp(self):
        DiscoverService.clear_cache()
        self.user = User.objects.create_user('user1', password='test123456')
        self.other = User.objects.create_user('user2', password='test123456')
        self.admin = User.objects.create_superuser(
            'admin', email=None, password='test123456'
        )
        self.user_token = _token(self.user)
        self.other_token = _token(self.other)
        self.admin_token = _token(self.admin)
        self.now = timezone.now()
        self.feed_ready = self._feed(
            'https://example.com/ready.xml',
            FeedStatus.READY,
            dt_checked=self.now,
            dryness=125,
            freeze_level=1,
        )
        self.feed_error = self._feed(
            'https://example.com/error.xml',
            FeedStatus.ERROR,
            dt_checked=self.now,
            dryness=250,
            freeze_level=2,
        )
        self.feed_stale = self._feed(
            'https://example.com/stale.xml',
            FeedStatus.READY,
            dt_checked=self.now - timedelta(days=31),
            dryness=750,
            freeze_level=4,
        )
        self.feed_unchecked = self._feed(
            'https://example.com/unchecked.xml',
            FeedStatus.READY,
            dt_checked=None,
            dryness=1000,
            freeze_level=8,
        )

    def _feed(self, url, status, *, dt_checked, dryness, freeze_level):
        feed = Feed(
            title=url.rsplit('/', 1)[-1],
            url=url,
            status=status,
            dt_updated=self.now,
            dt_checked=dt_checked,
            dt_latest_story_published=self.now,
            dryness=dryness,
            freeze_level=freeze_level,
        )
        feed.save()
        return feed

    def _post(self, path, data=None, token=None):
        headers = {}
        if token:
            headers['HTTP_AUTHORIZATION'] = f'Token {token}'
        return self.client.post(path, data or {}, **headers)

    def _create_category(self, slug='featured', name='精选', rank=0):
        response = self._post(
            '/api/v1/discover.admin_category_upsert',
            {'slug': slug, 'name': name, 'rank': rank},
            token=self.admin_token,
        )
        assert response.status_code == 200, response.content
        return response.json()['category']

    def _create_item(self, url, category='featured', editor_intro='简介', rank=0):
        response = self._post(
            '/api/v1/discover.admin_upsert',
            {
                'url': url,
                'category': category,
                'editor_intro': editor_intro,
                'rank': rank,
            },
            token=self.admin_token,
        )
        assert response.status_code == 200, response.content
        return response.json()['item']

    def test_permissions(self):
        response = self._post('/api/v1/discover.get', token=self.user_token)
        assert response.status_code == 200
        response = self._post('/api/v1/discover.get')
        assert response.status_code == 403

        response = self._post('/api/v1/discover.admin_categories')
        assert response.status_code == 403
        response = self._post(
            '/api/v1/discover.admin_categories',
            token=self.user_token,
        )
        assert response.status_code == 403
        response = self._post(
            '/api/v1/discover.admin_categories',
            token='invalid-token',
        )
        assert response.status_code == 403
        response = self._post(
            '/api/v1/discover.admin_categories', token=self.admin_token
        )
        assert response.status_code == 200

    def test_category_crud_and_soft_delete(self):
        self._create_category('tech', '科技资讯', 2)
        self._create_category('ai', 'AI 前沿', 1)
        response = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        assert [x['name'] for x in response['categories']] == ['AI 前沿', '科技资讯']

        response = self._post(
            '/api/v1/discover.admin_category_upsert',
            {'slug': 'ai', 'name': 'AI', 'rank': 0},
            token=self.admin_token,
        )
        assert response.status_code == 200
        response = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        assert [x['name'] for x in response['categories']] == ['AI', '科技资讯']

        self._create_item(self.feed_ready.url, category='ai')
        response = self._post(
            '/api/v1/discover.admin_category_delete',
            {'slug': 'ai'},
            token=self.admin_token,
        )
        assert response.status_code == 200
        deleted_at = response.json()['category']['deleted_at']
        assert deleted_at
        response = self._post(
            '/api/v1/discover.admin_category_delete',
            {'slug': 'ai'},
            token=self.admin_token,
        )
        assert response.json()['category']['deleted_at'] == deleted_at

        response = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        assert [x['slug'] for x in response['categories']] == ['tech']
        assert response['items'] == []
        response = self._post(
            '/api/v1/discover.admin_categories', token=self.admin_token
        ).json()
        assert response['categories'][0]['deleted_at'] is not None

        category = self._create_category('ai', 'AI 前沿', 1)
        assert category['deleted_at'] is None
        response = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        assert [x['slug'] for x in response['categories']] == ['ai', 'tech']
        assert [x['url'] for x in response['items']] == [self.feed_ready.url]

    def test_item_crud_and_validation(self):
        self._create_category('ai', 'AI 前沿')
        item = self._create_item(self.feed_ready.url, category='ai')
        assert item['category'] == 'ai'
        assert item['editor_intro'] == '简介'
        response = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        public_item = response['items'][0]
        assert 'feed_id' not in public_item
        assert public_item['url'] == self.feed_ready.url
        assert public_item['freeze_level'] == 1
        assert public_item['dryness'] == 125

        item = self._create_item(self.feed_ready.url, category='ai', editor_intro='新简介')
        assert item['editor_intro'] == '新简介'
        assert DiscoverFeed.objects.count() == 1

        response = self._post(
            '/api/v1/discover.admin_remove',
            {'url': self.feed_ready.url, 'category': 'ai'},
            token=self.admin_token,
        )
        assert response.status_code == 200
        assert response.json()['item']['deleted_at']
        response = self._post('/api/v1/discover.get', token=self.user_token)
        assert response.json()['items'] == []
        response = self._post(
            '/api/v1/discover.admin_list', token=self.admin_token
        ).json()
        assert response['total'] == 0
        response = self._post(
            '/api/v1/discover.admin_list',
            {'include_deleted': True},
            token=self.admin_token,
        ).json()
        assert response['total'] == 1
        assert response['items'][0]['deleted_at']

        item = self._create_item(self.feed_ready.url, category='ai')
        assert item['deleted_at'] is None
        response = self._post('/api/v1/discover.get', token=self.user_token)
        assert len(response.json()['items']) == 1

        response = self._post(
            '/api/v1/discover.admin_upsert',
            {'url': 'https://example.com/not-exist.xml', 'category': 'ai'},
            token=self.admin_token,
        )
        assert response.status_code == 400
        response = self._post(
            '/api/v1/discover.admin_upsert',
            {'url': self.feed_ready.url, 'category': 'not-exist'},
            token=self.admin_token,
        )
        assert response.status_code == 400
        response = self._post(
            '/api/v1/discover.admin_upsert',
            {
                'url': self.feed_ready.url,
                'category': 'ai',
                'editor_intro': 'x' * 501,
            },
            token=self.admin_token,
        )
        assert response.status_code == 400
        response = self._post(
            '/api/v1/discover.admin_category_upsert',
            {'slug': '   ', 'name': '空'},
            token=self.admin_token,
        )
        assert response.status_code == 400

        self._post(
            '/api/v1/discover.admin_category_delete',
            {'slug': 'ai'},
            token=self.admin_token,
        )
        response = self._post(
            '/api/v1/discover.admin_upsert',
            {'url': self.feed_ready.url, 'category': 'ai'},
            token=self.admin_token,
        )
        assert response.status_code == 400

    def test_public_filters(self):
        self._create_category('ai', 'AI 前沿')
        for feed in (
            self.feed_ready,
            self.feed_error,
            self.feed_stale,
            self.feed_unchecked,
        ):
            self._create_item(feed.url, category='ai')
        response = self._post('/api/v1/discover.get', token=self.user_token).json()
        assert [x['url'] for x in response['items']] == [self.feed_ready.url]

        self._post(
            '/api/v1/discover.admin_remove',
            {'url': self.feed_ready.url, 'category': 'ai'},
            token=self.admin_token,
        )
        response = self._post('/api/v1/discover.get', token=self.user_token).json()
        assert response['items'] == []

    def test_cache_shared_and_invalidated(self):
        assert DISCOVER_CACHE_TTL == 10 * 60
        self._create_category('ai', 'AI 前沿')
        self._create_item(self.feed_ready.url, category='ai')
        DiscoverService.clear_cache()
        first = self._post('/api/v1/discover.get', token=self.user_token)
        assert first.status_code == 200
        first_json = first.json()

        with CaptureQueriesContext(connection) as ctx:
            second = self._post('/api/v1/discover.get', token=self.other_token)
        assert second.status_code == 200
        assert second.json() == first_json
        discover_sql = [
            query['sql']
            for query in ctx.captured_queries
            if 'rssant_api_discover' in query['sql']
        ]
        assert discover_sql == []

        self._create_category('tech', '科技资讯', 2)
        third = self._post('/api/v1/discover.get', token=self.user_token)
        assert [x['slug'] for x in third.json()['categories']] == ['ai', 'tech']

    def test_url_matches_subscription_list(self):
        UserFeed(
            user=self.user,
            feed=self.feed_ready,
            dt_updated=self.now,
        ).save()
        self._create_category('ai', 'AI 前沿')
        self._create_item(self.feed_ready.url, category='ai')

        discover = self._post(
            '/api/v1/discover.get', token=self.user_token
        ).json()
        feeds = self._post('/api/v1/feed.query', token=self.user_token).json()
        assert discover['items'][0]['url'] == feeds['feeds'][0]['url']

    def test_admin_list_filters_and_pagination(self):
        self._create_category('a', 'A', 1)
        self._create_category('b', 'B', 2)
        self._create_item(self.feed_ready.url, category='a')
        self._create_item(self.feed_error.url, category='a')
        self._create_item(self.feed_stale.url, category='b')

        response = self._post(
            '/api/v1/discover.admin_list', token=self.admin_token
        ).json()
        assert response['total'] == 3
        assert response['page'] == 1
        assert len(response['items']) == 3
        assert {'category', 'url', 'status', 'dt_checked', 'dryness', 'freeze_level'} <= set(
            response['items'][0]
        )

        response = self._post(
            '/api/v1/discover.admin_list',
            {'category': 'a'},
            token=self.admin_token,
        ).json()
        assert response['total'] == 2
        assert {x['category'] for x in response['items']} == {'a'}

        response = self._post(
            '/api/v1/discover.admin_list',
            {'page': 2},
            token=self.admin_token,
        ).json()
        assert response['total'] == 3
        assert response['items'] == []

        response = self._post(
            '/api/v1/discover.admin_list',
            {'category': 'not-exist'},
            token=self.admin_token,
        )
        assert response.status_code == 400

    def test_admin_list_hides_items_of_deleted_category(self):
        self._create_category('ai', 'AI 前沿')
        self._create_item(self.feed_ready.url, category='ai')
        self._post(
            '/api/v1/discover.admin_category_delete',
            {'slug': 'ai'},
            token=self.admin_token,
        )
        response = self._post(
            '/api/v1/discover.admin_list', token=self.admin_token
        ).json()
        assert response['total'] == 0
        response = self._post(
            '/api/v1/discover.admin_list',
            {'include_deleted': True},
            token=self.admin_token,
        ).json()
        assert response['total'] == 1

    def test_empty_public_data(self):
        response = self._post('/api/v1/discover.get', token=self.user_token)
        assert response.status_code == 200
        assert response.json() == {'categories': [], 'items': []}
