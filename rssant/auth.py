from django.urls import path, include
from allauth.socialaccount.providers.github.views import GitHubOAuth2Adapter
from allauth.account.adapter import DefaultAccountAdapter
from rest_auth.registration.views import SocialConnectView
from rest_auth.registration.views import SocialLoginView
from rest_auth.registration.views import (
    SocialAccountListView, SocialAccountDisconnectView
)


class GitHubLogin(SocialLoginView):
    adapter_class = GitHubOAuth2Adapter


class GitHubConnect(SocialConnectView):
    adapter_class = GitHubOAuth2Adapter


class RssantAccountAdapter(DefaultAccountAdapter):
    """RSSAnt Account Adapter"""


urlpaterns = [
    path('auth/', include('rest_auth.urls')),
    path('auth/registration/', include('rest_auth.registration.urls')),
    path('auth/github/', GitHubLogin.as_view()),
    path('auth/github/connect/', GitHubConnect.as_view()),
    path('auth/socialaccount/', SocialAccountListView.as_view()),
    path('auth/socialaccount/<int:pk>/disconnect/', SocialAccountDisconnectView.as_view())
]
