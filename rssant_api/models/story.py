from django.utils import timezone
from django.db import connection, transaction
from validr import T

from rssant_common.detail import Detail
from rssant_api.monthly_story_count import MonthlyStoryCount
from .helper import Model, ContentHashMixin, models, optional, User
from .feed import Feed, UserFeed


MONTH_18 = timezone.timedelta(days=18 * 30)
ONE_MONTH = timezone.timedelta(days=30)

StoryDetailSchema = T.detail.fields("""
    unique_id
    dt_published
    dt_updated
    dt_created
    dt_watched
    dt_favorited
""").extra_fields("""
    author
    image_url
    audio_url
    iframe_url
    dt_synced
    summary
    content
    content_hash_base64
""").default(False)

STORY_DETAIL_FEILDS = Detail.from_schema(False, StoryDetailSchema).exclude_fields
USER_STORY_DETAIL_FEILDS = [f'story__{x}' for x in STORY_DETAIL_FEILDS]


class Story(Model, ContentHashMixin):
    """故事"""

    class Meta:
        unique_together = (
            ('feed', 'offset'),
            ('feed', 'unique_id'),
        )
        indexes = [
            models.Index(fields=["feed", "offset"]),
            models.Index(fields=["feed", "unique_id"]),
        ]

    class Admin:
        display_fields = ['feed_id', 'offset', 'title', 'link']

    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    offset = models.IntegerField(help_text="Story在Feed中的位置")
    unique_id = models.CharField(max_length=200, help_text="Unique ID")
    title = models.CharField(max_length=200, help_text="标题")
    link = models.TextField(help_text="文章链接")
    author = models.CharField(max_length=200, **optional, help_text='作者')
    image_url = models.TextField(
        **optional, help_text="图片链接")
    audio_url = models.TextField(
        **optional, help_text="播客音频链接")
    iframe_url = models.TextField(
        **optional, help_text="视频iframe链接")
    has_mathjax = models.BooleanField(
        **optional, default=False, help_text='has MathJax')
    is_user_marked = models.BooleanField(
        **optional, default=False, help_text='is user favorited or watched ever')
    dt_published = models.DateTimeField(help_text="发布时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_synced = models.DateTimeField(**optional, help_text="最近一次同步时间")
    summary = models.TextField(**optional, help_text="摘要或较短的内容")
    content = models.TextField(**optional, help_text="文章内容")

    @staticmethod
    def get_by_offset(feed_id, offset, detail=False) -> 'Story':
        q = Story.objects.filter(feed_id=feed_id, offset=offset)
        if not detail:
            q = q.defer(*STORY_DETAIL_FEILDS)
        return q.get()

    @staticmethod
    def _dedup_sort_storys(storys):
        # 去重，排序，分配offset时保证offset和dt_published顺序一致
        unique_storys = {}
        for story in storys:
            unique_id = story['unique_id']
            if unique_id in unique_storys:
                is_newer = story['dt_published'] > unique_storys[unique_id]['dt_published']
                if is_newer:
                    unique_storys[unique_id] = story
            else:
                unique_storys[unique_id] = story

        def key_func(x):
            return (x['dt_published'], x['unique_id'])

        storys = list(sorted(unique_storys.values(), key=key_func))
        return storys

    @staticmethod
    def bulk_save_by_feed(feed_id, storys, batch_size=100, is_refresh=False):
        if not storys:
            return []  # modified_story_objects
        storys = Story._dedup_sort_storys(storys)
        with transaction.atomic():
            feed = Feed.objects\
                .only(
                    '_version',
                    'id',
                    'dryness',
                    'monthly_story_count_data',
                    'total_storys',
                    'dt_first_story_published',
                    'dt_latest_story_published',
                )\
                .get(pk=feed_id)
            offset = feed.total_storys
            unique_ids = [x['unique_id'] for x in storys]
            story_objects = {}
            q = Story.objects\
                .defer(
                    'content', 'summary', 'title', 'author',
                    'image_url', 'iframe_url', 'audio_url',
                )\
                .filter(feed_id=feed_id, unique_id__in=unique_ids)
            for story in q.all():
                story_objects[story.unique_id] = story
            new_story_objects = []
            modified_story_objects = []
            now = timezone.now()
            for data in storys:
                unique_id = data['unique_id']
                content_hash_base64 = data['content_hash_base64']
                is_story_exist = unique_id in story_objects
                if is_story_exist:
                    story = story_objects[unique_id]
                    if (not is_refresh) and (not story.is_modified(content_hash_base64)):
                        continue
                else:
                    story = Story(feed_id=feed_id, unique_id=unique_id, offset=offset)
                    story_objects[unique_id] = story
                    new_story_objects.append(story)
                    offset += 1
                story.content_hash_base64 = content_hash_base64
                story.content = data['content']
                story.summary = data['summary']
                story.title = data["title"]
                story.link = data["link"]
                story.author = data["author"]
                story.image_url = data['image_url']
                story.iframe_url = data['iframe_url']
                story.audio_url = data['audio_url']
                story.has_mathjax = data['has_mathjax']
                # 发布时间只第一次赋值，不更新
                if not story.dt_published:
                    story.dt_published = data['dt_published']
                story.dt_updated = data['dt_updated']
                story.dt_synced = now
                if is_story_exist:
                    story.save()
                modified_story_objects.append(story)
            if new_story_objects:
                Story.objects.bulk_create(new_story_objects, batch_size=batch_size)
                Story._update_feed_monthly_story_count(feed, new_story_objects)
                Story._update_feed_story_dt_published_total_storys(feed, total_storys=offset)
            return modified_story_objects

    @staticmethod
    def _update_feed_monthly_story_count(feed, new_story_objects):
        monthly_story_count = MonthlyStoryCount.load(feed.monthly_story_count_data)
        for story in new_story_objects:
            if not story.dt_published:
                continue
            year, month = story.dt_published.year, story.dt_published.month
            if not MonthlyStoryCount.is_valid_year_month(year, month):
                continue
            count = monthly_story_count.get(year, month)
            monthly_story_count.put(year, month, count + 1)
        feed.monthly_story_count = monthly_story_count
        feed.save()

    @staticmethod
    def refresh_feed_monthly_story_count(feed_id):
        count_sql = """
        SELECT
            CAST(EXTRACT(YEAR FROM dt_published) AS INTEGER) AS year,
            CAST(EXTRACT(MONTH FROM dt_published) AS INTEGER) AS month,
            count(1) as count
        FROM rssant_api_story
        WHERE feed_id = %s AND dt_published IS NOT NULL
        GROUP BY
            CAST(EXTRACT(YEAR FROM dt_published) AS INTEGER),
            CAST(EXTRACT(MONTH FROM dt_published) AS INTEGER);
        """
        with connection.cursor() as cursor:
            cursor.execute(count_sql, [feed_id])
            rows = list(cursor.fetchall())
        items = []
        for row in rows:
            year, month, count = map(int, row)
            if 1970 <= year <= 9999:
                items.append((year, month, count))
        monthly_story_count = MonthlyStoryCount(items)
        with transaction.atomic():
            feed = Feed.objects.filter(pk=feed_id).get()
            feed.monthly_story_count = monthly_story_count
            feed.save()

    @staticmethod
    def _update_feed_story_dt_published_total_storys(feed, total_storys):
        if total_storys <= 0:
            return
        first_story = Story.objects\
            .only('id', 'offset', 'dt_published')\
            .filter(feed_id=feed.id, offset=0)\
            .first()
        latest_story = Story.objects\
            .only('id', 'offset', 'dt_published')\
            .filter(feed_id=feed.id, offset=total_storys - 1)\
            .first()
        feed.total_storys = total_storys
        if first_story:
            feed.dt_first_story_published = first_story.dt_published
        if latest_story:
            feed.dt_latest_story_published = latest_story.dt_published
        feed.save()

    @staticmethod
    def fix_feed_total_storys(feed_id):
        with transaction.atomic():
            feed = Feed.objects.only('_version', 'id', 'total_storys').get(pk=feed_id)
            total_storys = Story.objects.filter(feed_id=feed_id).count()
            if feed.total_storys != total_storys:
                feed.total_storys = total_storys
                feed.save()
                return True
            return False

    @staticmethod
    def query_feed_incorrect_total_storys():
        sql = """
        SELECT id, total_storys, correct_total_storys
        FROM rssant_api_feed AS current
        JOIN (
            SELECT feed_id, count(1) AS correct_total_storys
            FROM rssant_api_story
            GROUP BY feed_id
        ) AS correct
        ON current.id=correct.feed_id
        WHERE total_storys!=correct_total_storys
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())

    @staticmethod
    def set_user_marked_by_id(story_id, is_user_marked=True):
        Story.objects.filter(pk=story_id)\
            .update(is_user_marked=is_user_marked)

    @staticmethod
    def delete_by_retention_offset(feed_id, retention_offset):
        """
        delete storys < retention_offset and not is_user_marked
        """
        n, __ = Story.objects\
            .filter(feed_id=feed_id, offset__lt=retention_offset)\
            .exclude(is_user_marked=True)\
            .delete()
        return n

    @staticmethod
    def delete_by_retention(feed_id, retention=5000, limit=5000):
        """
        Params:
            feed_id: feed ID
            retention: num storys to keep
            limit: delete at most limit rows
        """
        with transaction.atomic():
            feed = Feed.get_by_pk(feed_id)
            offset = feed.retention_offset or 0
            # delete at most limit rows, avoid out of memory and timeout
            new_offset = min(offset + limit, feed.total_storys - retention)
            if new_offset > offset:
                n = Story.delete_by_retention_offset(feed_id, new_offset)
                feed.retention_offset = new_offset
                feed.save()
                return n
        return 0


class UserStory(Model):
    class Meta:
        unique_together = [
            ('user', 'story'),
            ('user_feed', 'offset'),
            ('user', 'feed', 'offset'),
        ]
        indexes = [
            models.Index(fields=["user", "feed", "offset"]),
            models.Index(fields=["user", "feed", "story"]),
        ]

    class Admin:
        display_fields = ['user_id', 'feed_id', 'story_id', 'is_watched', 'is_favorited']
        search_fields = ['user_feed_id']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    story = models.ForeignKey(Story, on_delete=models.CASCADE)
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    user_feed = models.ForeignKey(UserFeed, on_delete=models.CASCADE)
    offset = models.IntegerField(help_text="Story在Feed中的位置")
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    is_watched = models.BooleanField(default=False)
    dt_watched = models.DateTimeField(**optional, help_text="关注时间")
    is_favorited = models.BooleanField(default=False)
    dt_favorited = models.DateTimeField(**optional, help_text="标星时间")

    @staticmethod
    def get_by_pk(pk, user_id=None, detail=False):
        q = UserStory.objects.select_related('story')
        if not detail:
            q = q.defer(*USER_STORY_DETAIL_FEILDS)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        user_story = q.get(pk=pk)
        return user_story

    @staticmethod
    def get_by_offset(user_id, feed_id, offset, detail=False):
        q = UserStory.objects.select_related('story')
        q = q.filter(user_id=user_id, feed_id=feed_id, offset=offset)
        if not detail:
            q = q.defer(*USER_STORY_DETAIL_FEILDS)
        user_story = q.get()
        return user_story
