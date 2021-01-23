from allauth.socialaccount.providers.oauth2.views import (
    OAuth2View, OAuth2CallbackView, OAuth2LoginView)

from rssant_config import CONFIG
from rssant_config.env import GitHubConfigModel
from rssant_common.standby_domain import get_request_domain
from .client import RssantOAuth2Client


class RssantOAuth2View(OAuth2View):
    """
    TODO: avoid copy code from OAuth2View
    """

    def _get_client_id_secret(self, request, app):
        """select github config by request domain"""
        domain = get_request_domain(request)
        configs_parsed = CONFIG.github_standby_configs_parsed
        cfg: GitHubConfigModel = configs_parsed.get(domain)
        if cfg:
            return (cfg.client_id, cfg.secret)
        else:
            return (app.client_id, app.secret)

    def get_client(self, request, app):
        callback_url = self.adapter.get_callback_url(request, app)
        provider = self.adapter.get_provider()
        scope = provider.get_scope(request)
        client_id, secret = self._get_client_id_secret(request, app)
        client = RssantOAuth2Client(
            self.request, client_id, secret,
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
