import pytest

from rssant_feedlib import FeedContentType, FeedResponse, FeedResponseStatus
from rssant_worker.feed_creation_error import (
    ACCESS_DENIED,
    CONNECTION_ERROR,
    HTML_PAGE,
    NOT_A_FEED,
    NOT_FOUND,
    PRIVATE_ADDRESS,
    SSL_ERROR,
    TEMPORARY_UNAVAILABLE,
    UNKNOWN,
    describe_feed_creation_error,
)


def _response(status, feed_type=FeedContentType.OTHER):
    return FeedResponse(status=status, feed_type=feed_type)


@pytest.mark.parametrize(
    'status, expected',
    [
        (404, NOT_FOUND),
        (410, NOT_FOUND),
        (401, ACCESS_DENIED),
        (403, ACCESS_DENIED),
        (FeedResponseStatus.REFERER_DENY.value, ACCESS_DENIED),
        (FeedResponseStatus.REFERER_NOT_ALLOWED.value, ACCESS_DENIED),
        (429, TEMPORARY_UNAVAILABLE),
        (500, TEMPORARY_UNAVAILABLE),
        (503, TEMPORARY_UNAVAILABLE),
        (FeedResponseStatus.SSL_ERROR.value, SSL_ERROR),
        (FeedResponseStatus.DNS_ERROR.value, CONNECTION_ERROR),
        (FeedResponseStatus.CONNECTION_ERROR.value, CONNECTION_ERROR),
        (FeedResponseStatus.CONNECTION_TIMEOUT.value, CONNECTION_ERROR),
        (FeedResponseStatus.READ_TIMEOUT.value, CONNECTION_ERROR),
        (FeedResponseStatus.CONNECTION_RESET.value, CONNECTION_ERROR),
        (FeedResponseStatus.PROXY_ERROR.value, CONNECTION_ERROR),
        (FeedResponseStatus.RSS_PROXY_ERROR.value, CONNECTION_ERROR),
        (FeedResponseStatus.TOO_MANY_REDIRECT_ERROR.value, CONNECTION_ERROR),
        (FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value, PRIVATE_ADDRESS),
        (418, UNKNOWN),
        (FeedResponseStatus.RESPONSE_ERROR.value, UNKNOWN),
        (FeedResponseStatus.CONTENT_TYPE_NOT_SUPPORT_ERROR.value, UNKNOWN),
    ],
)
def test_status_categories(status, expected):
    assert describe_feed_creation_error(_response(status)) == expected


@pytest.mark.parametrize(
    'status, feed_type, expected',
    [
        (200, FeedContentType.HTML, HTML_PAGE),
        (200, FeedContentType.OTHER, NOT_A_FEED),
        (200, FeedContentType.XML, NOT_A_FEED),
        (200, FeedContentType.JSON, NOT_A_FEED),
    ],
)
def test_ok_response_by_content_type(status, feed_type, expected):
    assert describe_feed_creation_error(_response(status, feed_type)) == expected


def test_status_takes_priority_over_content_type():
    assert describe_feed_creation_error(
        _response(403, FeedContentType.HTML)
    ) == ACCESS_DENIED
    assert describe_feed_creation_error(
        _response(404, FeedContentType.HTML)
    ) == NOT_FOUND


def test_no_response_falls_back():
    assert describe_feed_creation_error(None) == UNKNOWN
