import os.path
from urllib.parse import urljoin

from django.urls import path, include
from django.template import Template, RequestContext
from django.core.mail import send_mail

import pynliner
from html2text import html2text
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from allauth.account.adapter import DefaultAccountAdapter
from rest_auth.registration.views import SocialConnectView
from rest_auth.registration.views import SocialLoginView
from rest_auth.registration.views import (
    SocialAccountListView, SocialAccountDisconnectView
)

from rssant.settings import BASE_DIR, ENV_CONFIG


class EmailTemplate:
    def __init__(self, subject, filename):
        filepath = os.path.join(BASE_DIR, 'rssant/templates/email', filename)
        with open(filepath) as f:
            html = f.read()
        text = html2text(html)
        self.text_template = Template(text)
        html = pynliner.fromString(html)
        self.html_template = Template(html)
        self.subject = subject

    def send(self, sender, receiver, ctx):
        text = self.text_template.render(ctx)
        html = self.html_template.render(ctx)
        send_mail(self.subject, text, sender, [receiver],
                  fail_silently=False, html_message=html)


EMAIL_CONFIRM_TEMPLATE = EmailTemplate(
    subject='[蚁阅] 请验证您的邮箱',
    filename='confirm.html',
)


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
        ctx = RequestContext(request, ctx)
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
