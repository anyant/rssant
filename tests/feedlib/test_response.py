from rssant_feedlib.response import FeedResponseStatus, FeedResponse, FeedContentType


def test_response_status():
    status = FeedResponseStatus(-200)
    assert status == -200
    assert status in (-200, -300)
    assert FeedResponseStatus.name_of(200) == 'OK'
    assert FeedResponseStatus.name_of(600) == 'HTTP_600'
    assert FeedResponseStatus.name_of(-200) == 'FEED_CONNECTION_ERROR'
    assert FeedResponseStatus.is_need_proxy(-200)


def test_response_repr():
    response = FeedResponse(
        content=b'123456',
        url='https://example.com/feed.xml',
        feed_type=FeedContentType.XML,
    )
    assert repr(response)
    response = FeedResponse(
        url='https://example.com/feed.xml',
    )
    assert repr(response)
