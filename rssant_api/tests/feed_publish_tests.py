import pytest
from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone

from rssant_api.models import Feed, FeedStatus, UserFeed
from rssant_api.models.user_publish import UserPublish


@pytest.mark.dbtest
class FeedPublishTestCase(TestCase):
    def setUp(self):
        user = User.objects.create_user(
            username='testuser',
            email=None,
            password='test123456',
        )
        self._user = user
        is_success = self.client.login(
            username='testuser',
            password='test123456',
        )
        self.assertTrue(is_success, 'login failed')

        feed_s = []
        for i in range(2):
            feed = Feed(
                title=f'test feed{i}',
                url=f'https://blog.example.com/feed{i}.xml',
                status=FeedStatus.READY,
                dt_updated=timezone.now(),
            )
            feed.save()
            feed_s.append(feed)
        self._feed1 = feed_s[0]
        self._feed2 = feed_s[1]

        UserFeed(
            user_id=user.id,
            feed_id=self._feed1.id,
            title=self._feed1.title,
            is_publish=True,
        ).save()

        UserFeed(
            user_id=user.id,
            feed_id=self._feed2.id,
            title=self._feed2.title,
            is_publish=False,
        ).save()

    def test_not_enable(self):
        # test /api/v1/user_publish.get
        response = self.client.post(
            '/api/v1/user_publish.get',
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is False
        assert not response.json()['unionid']

        # test /api/v1/user_publish.set
        response = self.client.post(
            '/api/v1/user_publish.set',
            {'is_enable': True},
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is True
        unionid = response.json()['unionid']
        assert unionid

        # test /api/v1/user_publish.set
        response = self.client.post(
            '/api/v1/user_publish.set',
            {'is_enable': False, 'unionid': unionid},
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is False
        assert response.json()['unionid'] == unionid

        # test /api/v1/publish.info
        UserPublish.internal_clear_cache()
        response = self.client.post(
            '/api/v1/publish.info',
            HTTP_X_RSSANT_PUBLISH=unionid,
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is False
        assert response.json()['unionid'] == unionid

    def test_publish_feed_query(self):
        response = self.client.post(
            '/api/v1/user_publish.get',
        )
        assert response.status_code == 200
        unionid = response.json()['unionid']

        response = self.client.post(
            '/api/v1/user_publish.set',
            {
                'unionid': unionid,
                'is_enable': True,
                'is_all_public': False,
            },
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is True
        unionid = response.json()['unionid']
        assert unionid

        UserPublish.internal_clear_cache()
        response = self.client.post(
            '/api/v1/publish.feed_query',
            HTTP_X_RSSANT_PUBLISH=unionid,
        )
        assert response.status_code == 200
        assert len(response.json()['feeds']) == 1

        response = self.client.post(
            '/api/v1/user_publish.set',
            {
                'unionid': unionid,
                'is_enable': True,
                'is_all_public': True,
            },
        )
        assert response.status_code == 200
        assert response.json()['is_enable'] is True
        unionid = response.json()['unionid']
        assert unionid

        UserPublish.internal_clear_cache()
        response = self.client.post(
            '/api/v1/publish.feed_query',
            HTTP_X_RSSANT_PUBLISH=unionid,
        )
        assert response.status_code == 200
        assert len(response.json()['feeds']) == 2
