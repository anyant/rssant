from allauth.socialaccount.providers.oauth2.urls import default_urlpatterns

from .provider import RssantGitHubProvider

urlpatterns = default_urlpatterns(RssantGitHubProvider)
