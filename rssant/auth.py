from urllib.parse import urljoin

from django.urls import path, include

from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from allauth.account.adapter import DefaultAccountAdapter
from rest_auth.registration.views import SocialConnectView
from rest_auth.registration.views import SocialLoginView
from rest_auth.registration.views import (
    SocialAccountListView, SocialAccountDisconnectView
)

from rssant.settings import ENV_CONFIG
from .email_template import EMAIL_CONFIRM_TEMPLATE


class GitHubLogin(SocialLoginView):
    adapter_class = GitHubOAuth2Adapter


class GitHubConnect(SocialConnectView):
    adapter_class = GitHubOAuth2Adapter


class RssantAccountAdapter(DefaultAccountAdapter):
    """RSSAnt Account Adapter"""

    def get_email_confirmation_url(self, request, emailconfirmation):
        """Constructs the email confirmation (activation) url.

        Note that if you have architected your system such that email
        confirmations are sent outside of the request context `request`
        can be `None` here.
        """
        url = 'account-confirm-email/{}'.format(emailconfirmation.key)
        return urljoin(ENV_CONFIG.root_url, url)

    def send_confirmation_mail(self, request, emailconfirmation, signup):
        username = emailconfirmation.email_address.user.username
        link = self.get_email_confirmation_url(request, emailconfirmation)
        ctx = {
            "link": link,
            "username": username,
            "user": emailconfirmation.email_address.user,
            "key": emailconfirmation.key,
        }
        sender = self.get_from_email()
        receiver = emailconfirmation.email_address.email
        EMAIL_CONFIRM_TEMPLATE.send(sender, receiver, ctx)


urlpaterns = [
    path('auth/', include('rest_auth.urls')),
    path('auth/registration/', include('rest_auth.registration.urls')),
    path('auth/github/', GitHubLogin.as_view()),
    path('auth/github/connect/', GitHubConnect.as_view()),
    path('auth/socialaccount/', SocialAccountListView.as_view()),
    path('auth/socialaccount/<int:pk>/disconnect/', SocialAccountDisconnectView.as_view())
]
