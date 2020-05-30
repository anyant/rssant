from django.utils import timezone
from django.test import TransactionTestCase
from validr import T

from rssant_common.validator import compiler
from rssant_api.models import Feed, FeedStatus
from rssant_api.models import STORY_SERVICE, Story
from rssant.helper.content_hash import compute_hash_base64


StorySchema = T.dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str,
    author=T.str.optional,
    link=T.str.optional,
    image_url=T.url.optional,
    iframe_url=T.url.optional,
    audio_url=T.url.optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

validate_story = compiler.compile(StorySchema)


class StoryTestCase(TransactionTestCase):

    def setUp(self):
        print('setUp')
        storys = []
        updated_storys = []
        now = timezone.datetime(2020, 6, 1, 12, 12, 12)
        for i in range(200):
            dt = now + timezone.timedelta(minutes=i)
            content = f'test story content {i}' * (i % 5)
            content_hash_base64 = compute_hash_base64(content)
            summary = content[:30]
            story = {
                'unique_id': f'blog.example.com/{i}',
                'title': f'test story {i}',
                'content_hash_base64': content_hash_base64,
                'author': 'tester',
                'link': f'https://blog.example.com/{i}.html',
                'dt_published': dt,
                'dt_updated': dt,
                'summary': summary,
                'content': content,
            }
            storys.append(validate_story(story))
            updated_story = dict(story)
            updated_content = f'test story content updated {i}' * (i % 5 + 1)
            updated_story.update(
                content=updated_content,
                content_hash_base64=compute_hash_base64(updated_content),
            )
            updated_storys.append(validate_story(updated_story))
        self.storys = storys
        self.updated_storys = updated_storys

        feed = Feed(
            title='test feed',
            url='https://blog.example.com/feed.xml',
            status=FeedStatus.READY,
            dt_updated=timezone.now(),
        )
        feed.save()
        self.feed_id = feed.id

    def test_new_bulk_save_by_feed(self):
        storys_0_30 = self.storys[:30]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)

        storys_20_50 = self.storys[20:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_20_50, batch_size=10)
        self.assertEqual(len(modified), 20)

        updated_storys_30_50 = self.updated_storys[30:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, updated_storys_30_50, batch_size=10)
        self.assertEqual(len(modified), 20)

    def test_mix_bulk_save_by_feed(self):
        storys_0_30 = self.storys[:30]
        modified = Story.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)

        storys_10_50 = self.updated_storys[10:30] + self.storys[30:50]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_10_50, batch_size=10)
        self.assertEqual(len(modified), 40)

        storys_40_60 = self.storys[40:60]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_40_60, batch_size=10)
        self.assertEqual(len(modified), 10)

    def test_bulk_save_by_feed_refresh(self):
        storys_0_20 = self.storys[:20]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 20)

        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 0)

        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10, is_refresh=True)
        self.assertEqual(len(modified), 20)

    def test_update_story(self):
        storys_0_20 = self.storys[:20]
        modified = STORY_SERVICE.bulk_save_by_feed(
            self.feed_id, storys_0_20, batch_size=10)
        self.assertEqual(len(modified), 20)

        story_10 = self.updated_storys[10]
        data = {k: story_10[k] for k in ['content', 'summary', 'dt_published']}
        STORY_SERVICE.update_story(self.feed_id, 10, data)

    def test_delete_by_retention(self):
        storys_0_30 = self.storys[:30]
        modified = Story.bulk_save_by_feed(
            self.feed_id, storys_0_30, batch_size=10)
        self.assertEqual(len(modified), 30)

        storys_20_50 = self.storys[20:50]
        modified = Story.bulk_save_by_feed(
            self.feed_id, storys_20_50, batch_size=10)
        self.assertEqual(len(modified), 20)

        n = STORY_SERVICE.delete_by_retention(self.feed_id, retention=10, limit=10)
        self.assertEqual(n, 10)

        n = STORY_SERVICE.delete_by_retention(self.feed_id, retention=10, limit=50)
        self.assertEqual(n, 30)
