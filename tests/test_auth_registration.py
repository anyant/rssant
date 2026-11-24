from unittest import mock

import pytest
from allauth.socialaccount.helpers import complete_social_login
from allauth.socialaccount.models import SocialAccount, SocialLogin
from django.contrib.auth.models import User
from django.contrib.sessions.backends.db import SessionStore
from django.test import RequestFactory, TestCase
from rest_framework.authtoken.models import Token

from rssant.auth import RssantAccountAdapter
from rssant_config import CONFIG

CLOSED_MESSAGE = '本站未开放注册'
PASSWORD = 'Abcd123456!'


def _social_login(uid, username, email):
    user = User(username=username, email=email)
    account = SocialAccount(provider='github', uid=uid)
    return SocialLogin(user=user, account=account)


def _request():
    request = RequestFactory().get('/api/v1/accounts/github/login/callback/')
    request.session = SessionStore()
    request.user = mock.Mock(is_authenticated=False)
    request._messages = mock.Mock()
    return request


@pytest.mark.dbtest
class UserRegistrationTests(TestCase):
    def _register(self, username='newuser'):
        return self.client.post('/api/v1/auth/registration/', {
            'username': username,
            'email': username + '@example.com',
            'password1': PASSWORD,
            'password2': PASSWORD,
        })

    def test_registration_closed(self):
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = self._register()
        assert response.status_code == 403
        assert response.json() == {'message': CLOSED_MESSAGE}
        assert not User.objects.filter(username='newuser').exists()

    def test_registration_open(self):
        with mock.patch.object(CONFIG, 'user_registration_enable', True):
            response = self._register()
        assert response.status_code == 201
        assert User.objects.filter(username='newuser').exists()

    def test_is_open_for_signup_follows_config(self):
        adapter = RssantAccountAdapter()
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            assert adapter.is_open_for_signup(None) is False
        with mock.patch.object(CONFIG, 'user_registration_enable', True):
            assert adapter.is_open_for_signup(None) is True

    def test_signup_page_closed(self):
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = self.client.get('/api/v1/accounts/signup/')
        assert response.status_code == 200
        assert CLOSED_MESSAGE in response.content.decode('utf-8')

    def test_social_signup_closed(self):
        sociallogin = _social_login('uid-closed', 'socialnew', 'socialnew@example.com')
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = complete_social_login(_request(), sociallogin)
        assert response.status_code == 200
        assert CLOSED_MESSAGE in response.content.decode('utf-8')
        assert not User.objects.filter(username='socialnew').exists()
        assert not SocialAccount.objects.filter(uid='uid-closed').exists()

    def test_social_login_existing_user(self):
        user = User.objects.create_user('bound', email='bound@example.com', password=PASSWORD)
        SocialAccount.objects.create(user=user, provider='github', uid='uid-bound')
        sociallogin = _social_login('uid-bound', 'tmpuser', 'tmp@example.com')
        request = _request()
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = complete_social_login(request, sociallogin)
        assert response.status_code == 302
        assert request.session.get('_auth_user_id') == str(user.pk)
        assert not User.objects.filter(username='tmpuser').exists()

    def test_existing_user_login(self):
        User.objects.create_user('existing', email='existing@example.com', password=PASSWORD)
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = self.client.post('/api/v1/user/login/', {
                'account': 'existing',
                'password': PASSWORD,
            })
        assert response.status_code == 200
        assert response.json()['token']

    def test_existing_user_change_password(self):
        user = User.objects.create_user('changer', email='changer@example.com', password=PASSWORD)
        token = Token.objects.get_or_create(user=user)[0]
        with mock.patch.object(CONFIG, 'user_registration_enable', False):
            response = self.client.post('/api/v1/auth/password/change/', {
                'new_password1': 'NewPass1234!',
                'new_password2': 'NewPass1234!',
            }, HTTP_AUTHORIZATION='Token ' + token.key)
        assert response.status_code == 200
        user.refresh_from_db()
        assert user.check_password('NewPass1234!')
