from allauth.socialaccount.providers.oauth2.views import (
    OAuth2View, OAuth2CallbackView, OAuth2LoginView)

from .client import RssantOAuth2Client


class RssantOAuth2View(OAuth2View):
    """
    TODO: avoid copy code from OAuth2View
    """

    def get_client(self, request, app):
        callback_url = self.adapter.get_callback_url(request, app)
        provider = self.adapter.get_provider()
        scope = provider.get_scope(request)
        client = RssantOAuth2Client(
            self.request, app.client_id, app.secret,
            self.adapter.access_token_method,
            self.adapter.access_token_url,
            callback_url,
            scope,
            scope_delimiter=self.adapter.scope_delimiter,
            headers=self.adapter.headers,
            basic_auth=self.adapter.basic_auth
        )
        return client


class RssantOAuth2CallbackView(RssantOAuth2View, OAuth2CallbackView):
    """RssantOAuth2CallbackView"""


class RssantOAuth2LoginView(RssantOAuth2View, OAuth2LoginView):
    """RssantOAuth2LoginView"""
