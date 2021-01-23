from allauth.socialaccount import app_settings
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter

import yarl
from rssant_common.standby_domain import get_request_domain
from ..helper import oauth_api_request
from ..oauth2.views import RssantOAuth2CallbackView, RssantOAuth2LoginView


class RssantGitHubOAuth2Adapter(GitHubOAuth2Adapter):
    """
    switch to header authorization
    https://github.com/pennersr/django-allauth/pull/2458

    TODO: avoid copy code from GitHubOAuth2Adapter
    """

    def get_callback_url(self, request, app):
        url = super().get_callback_url(request, app)
        domain = get_request_domain(request)
        url = str(yarl.URL(url).with_host(domain))
        return url

    def complete_login(self, request, app, token, **kwargs):
        headers = {'Authorization': 'token {}'.format(token.token)}
        resp = oauth_api_request('GET', self.profile_url, headers=headers)
        resp.raise_for_status()
        extra_data = resp.json()
        if app_settings.QUERY_EMAIL and not extra_data.get('email'):
            extra_data['email'] = self.get_email(headers)
        return self.get_provider().sociallogin_from_response(
            request, extra_data
        )

    def get_email(self, headers):
        email = None
        resp = oauth_api_request('GET', self.emails_url, headers=headers)
        resp.raise_for_status()
        emails = resp.json()
        if resp.status_code == 200 and emails:
            email = emails[0]
            primary_emails = [
                e for e in emails
                if not isinstance(e, dict) or e.get('primary')
            ]
            if primary_emails:
                email = primary_emails[0]
            if isinstance(email, dict):
                email = email.get('email', '')
        return email


oauth2_login = RssantOAuth2LoginView.adapter_view(RssantGitHubOAuth2Adapter)
oauth2_callback = RssantOAuth2CallbackView.adapter_view(RssantGitHubOAuth2Adapter)
