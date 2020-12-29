import pytest
from django.utils import timezone
from django.test import TestCase
from django.contrib.auth.models import User

from rssant_api.models import Feed, FeedStatus, UnionFeed, FeedUrlMap, FeedCreation, FeedImportItem, UserFeed
from rssant_api.feed_helper import render_opml
from rssant_feedlib.importer import import_feed_from_text


@pytest.mark.dbtest
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


@pytest.mark.dbtest
class FeedImportTestCase(TestCase):
    def setUp(self):
        feed = Feed(
            title='测试1',
            url='https://blog.example.com/feed1.xml',
            status=FeedStatus.READY,
            dt_updated=timezone.now(),
        )
        feed.save()
        tester = User.objects.create_superuser('tester', email=None, password='test123456')
        self._tester = tester

    def _import_feeds(self, imports: list):
        result = UnionFeed.create_by_imports(user_id=self._tester.id, imports=imports)
        for creation in result.feed_creations:
            creation: FeedCreation
            feed = Feed(
                title=creation.title,
                url=creation.url,
                status=FeedStatus.READY,
                dt_updated=timezone.now(),
            )
            feed.save()
            user_feed = UserFeed(
                user=self._tester,
                feed=feed,
                title=creation.title,
                group=creation.group,
                dt_updated=timezone.now(),
            )
            user_feed.save()
            FeedUrlMap(source=creation.url, target=feed.url).save()
            FeedUrlMap(source=creation.url + '.c', target=feed.url).save()
        return result

    def _query_user_feeds(self):
        _, feeds, _ = UnionFeed.query_by_user(self._tester.id)
        return feeds

    def test_import_feeds(self):
        imports = [
            FeedImportItem(title='测试1', group=None, url='https://blog.example.com/feed1.xml'),
            FeedImportItem(title='测试2', group='', url='https://blog.example.com/feed2.xml'),
            FeedImportItem(title='测试3', group='品读', url='https://blog.example.com/feed3.xml'),
            FeedImportItem(title='测试4', group='设计', url='https://blog.example.com/feed4.xml'),
        ]
        self.assertEqual(Feed.objects.count(), 1, 'expect 1 feeds in database')

        result = self._import_feeds(imports[:1])
        msg = 'after insert 1 feeds'
        self.assertEqual(result.num_created_feeds, 1, msg)
        self.assertEqual(result.num_existed_feeds, 0, msg)
        self.assertEqual(result.num_feed_creations, 0, msg)
        self.assertEqual(Feed.objects.count(), 1, msg)

        user_feed_count = len(self._query_user_feeds())
        self.assertEqual(user_feed_count, 1, 'after subscribe 1 feeds')

        result = self._import_feeds(imports)
        msg = 'after insert 4 feeds'
        self.assertEqual(result.num_created_feeds, 0, msg)
        self.assertEqual(result.num_existed_feeds, 1, msg)
        self.assertEqual(result.num_feed_creations, 3, msg)
        self.assertEqual(Feed.objects.count(), 4, msg)

        user_feed_count = len(self._query_user_feeds())
        self.assertEqual(user_feed_count, 4, 'after subscribe 4 unique feeds')

        duplicate_imports = [
            FeedImportItem(title='测试2', group='', url='https://blog.example.com/feed2.xml'),
            FeedImportItem(title='测试2c', group='', url='https://blog.example.com/feed2.xml.c'),
            FeedImportItem(title='测试3c', group='品读', url='https://blog.example.com/feed3.xml.c'),
            FeedImportItem(title='测试4c', group='设计', url='https://blog.example.com/feed4.xml.c'),
        ]
        result = self._import_feeds(duplicate_imports)
        msg = 'after insert 4 duplicate feeds'
        self.assertEqual(result.num_created_feeds, 0, msg)
        self.assertEqual(result.num_existed_feeds, 3, msg)
        self.assertEqual(result.num_feed_creations, 0, msg)
        self.assertEqual(Feed.objects.count(), 4, msg)

        user_feed_count = len(self._query_user_feeds())
        self.assertEqual(user_feed_count, 4, 'after subscribe 4 duplicate feeds')

    def test_export_opml(self):
        imports = [
            FeedImportItem(title='测试1', group=None, url='https://blog.example.com/feed1.xml'),
            FeedImportItem(title='测试2', group='', url='https://blog.example.com/feed2.xml'),
            FeedImportItem(title='测试3', group='品读', url='https://blog.example.com/feed3.xml'),
            FeedImportItem(title='测试4', group='设计', url='https://blog.example.com/feed4.xml'),
        ]
        self._import_feeds(imports)
        self.assertEqual(Feed.objects.count(), 4)

        feeds = self._query_user_feeds()
        self.assertEqual(len(feeds), 4)
        content = render_opml(feeds)
        raw_imports = import_feed_from_text(content)
        self.assertEqual(len(raw_imports), 4)
