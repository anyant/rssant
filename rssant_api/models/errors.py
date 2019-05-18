from .helper import ConcurrentUpdateError


class RssantModelError(Exception):
    """Base exception for rssant API models"""


class FeedExistsError(RssantModelError):
    """Feed already exists"""


__all__ = (
    'ConcurrentUpdateError',
    'RssantModelError',
    'FeedExistsError',
)
