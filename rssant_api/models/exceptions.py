class RssantModelException(Exception):
    """Base exception for rssant API models"""


class FeedExistsException(RssantModelException):
    """Feed already exists"""
