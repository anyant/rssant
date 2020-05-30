from django.utils import timezone
from django.test import TestCase

from rssant_api.models import Feed, FeedStatus


class FeedSimpleTestCase(TestCase):
    def setUp(self):
        feed = Feed(
            title='test feed',
            url='https://blog.example.com/feed.xml',
            status=FeedStatus.READY,
            dt_updated=timezone.now(),
        )
        feed.save()

    def test_get_feed_by_url(self):
        url = 'https://blog.example.com/feed.xml'
        got = Feed.get_first_by_url(url)
        self.assertEqual(got.title, 'test feed')
