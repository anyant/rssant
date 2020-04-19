from allauth.socialaccount.providers.github.provider import GitHubProvider


class RssantGitHubProvider(GitHubProvider):
    """RssantGitHubProvider"""


provider_classes = [RssantGitHubProvider]
