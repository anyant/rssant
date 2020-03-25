import logging
from urllib.parse import urljoin

from django.urls import path, include

from django.contrib.auth.models import User
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from allauth.account.adapter import DefaultAccountAdapter
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from allauth.account.models import EmailAddress
from allauth.socialaccount.models import SocialLogin, SocialAccount
from rest_auth.registration.views import SocialConnectView
from rest_auth.registration.views import SocialLoginView
from rest_auth.registration.views import (
    SocialAccountListView, SocialAccountDisconnectView
)

from rssant_config import CONFIG
from .email_template import EMAIL_CONFIRM_TEMPLATE


LOG = logging.getLogger(__name__)


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
        return urljoin(CONFIG.root_url, url)

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


class RssantSocialAccountAdapter(DefaultSocialAccountAdapter):
    """
    connect existing user to new social account when a user login using a
    social account who already has registered using email.
    https://stackoverflow.com/questions/28897220/django-allauth-social-account-connect-to-existing-account-on-login
    """

    def _log_social_login(self, sociallogin: SocialLogin):
        emails = []
        if sociallogin.email_addresses:
            email: EmailAddress
            for email in sociallogin.email_addresses:
                if email.verified:
                    emails.append(f'{email.email} verified')
                else:
                    emails.append(f'{email.email} not-verified')
        emails = '[{}]'.format(', '.join(emails))
        if sociallogin.account:
            sa: SocialAccount = sociallogin.account
            account = f'[{sa.provider} pk={sa.pk}]'
        else:
            account = None
        if sociallogin.user:
            su: User = sociallogin.user
            user = f'[{su.username} email={su.email} pk={su.pk}]'
        else:
            user = None
        process = sociallogin.state.get('process')
        LOG.info(f'social-login account={account} user={user} emails={emails} process={process}')

    def pre_social_login(self, request, sociallogin: SocialLogin):
        self._log_social_login(sociallogin)
        # TODO: connect verified email and existing user


urlpaterns = [
    path('auth/', include('rest_auth.urls')),
    path('auth/registration/', include('rest_auth.registration.urls')),
    path('auth/github/', GitHubLogin.as_view()),
    path('auth/github/connect/', GitHubConnect.as_view()),
    path('auth/socialaccount/', SocialAccountListView.as_view()),
    path('auth/socialaccount/<int:pk>/disconnect/', SocialAccountDisconnectView.as_view())
]
