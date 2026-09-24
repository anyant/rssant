from unittest import mock

from django.http import HttpResponseRedirect
from django.test import RequestFactory

from rssant.allauth_providers.oauth2.views import RssantOAuth2CallbackView


def _dispatch_with_redirect(location, is_authenticated=True, token_key='token123'):
    request = RequestFactory().get('/api/v1/accounts/github/login/callback/')
    request.user = mock.Mock(is_authenticated=is_authenticated)
    view = RssantOAuth2CallbackView()
    view.adapter = mock.Mock()
    patches = [
        mock.patch(
            'allauth.socialaccount.providers.oauth2.views.OAuth2CallbackView.dispatch',
            return_value=HttpResponseRedirect(location),
        ),
    ]
    if is_authenticated:
        patches.append(mock.patch(
            'rssant.allauth_providers.oauth2.views.Token.objects.get_or_create',
            return_value=(mock.Mock(key=token_key), True),
        ))
    for p in patches:
        p.start()
    try:
        return view.dispatch(request)
    finally:
        for p in patches:
            p.stop()


def test_callback_appends_login_token():
    response = _dispatch_with_redirect('/')
    assert response.status_code == 302
    assert response['Location'] == '/#login_token=token123'


def test_callback_keeps_query_when_appending_token():
    response = _dispatch_with_redirect('/?a=1')
    assert response['Location'] == '/?a=1#login_token=token123'


def test_callback_skips_token_when_not_authenticated():
    response = _dispatch_with_redirect('/', is_authenticated=False)
    assert response['Location'] == '/'


def test_callback_skips_token_for_external_redirect():
    response = _dispatch_with_redirect('https://evil.com/')
    assert response['Location'] == 'https://evil.com/'
