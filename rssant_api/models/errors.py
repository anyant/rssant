from .helper import ConcurrentUpdateError


class RssantModelError(Exception):
    """Base exception for rssant API models"""


class FeedExistsError(RssantModelError):
    """Feed already exists"""


class FeedNotFoundError(RssantModelError):
    """Feed not found"""


class StoryNotFoundError(RssantModelError):
    """Story not found"""


class FeedStoryOffsetError(RssantModelError):
    """Feed story_offset error"""


__all__ = (
    'ConcurrentUpdateError',
    'RssantModelError',
    'FeedExistsError',
    'FeedNotFoundError',
    'StoryNotFoundError',
    'FeedStoryOffsetError',
)
