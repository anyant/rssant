from django.db import connection, transaction

from .helper import Model, ContentHashMixin, models, optional, User
from .feed import Feed, UserFeed


class Story(Model, ContentHashMixin):
    """故事"""

    class Meta:
        unique_together = ('feed', 'unique_id')
        indexes = [
            models.Index(fields=["feed", "dt_updated"]),
            models.Index(fields=["feed", "unique_id"]),
        ]

    class Admin:
        display_fields = ['feed_id', 'title', 'link']

    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    unique_id = models.CharField(max_length=200, help_text="Unique ID")
    title = models.CharField(max_length=200, **optional, help_text="标题")
    link = models.TextField(**optional, help_text="文章链接")
    author = models.CharField(max_length=200, **optional, help_text='作者')
    dt_published = models.DateTimeField(**optional, help_text="发布时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_synced = models.DateTimeField(**optional, help_text="最近一次同步时间")
    summary = models.TextField(**optional, help_text="摘要或较短的内容")
    content = models.TextField(**optional, help_text="文章内容")

    def to_dict(self, detail=False):
        ret = dict(
            unique_id=self.unique_id,
            title=self.title,
            link=self.link,
            dt_published=self.dt_published,
            dt_updated=self.dt_updated,
            dt_created=self.dt_created,
            dt_synced=self.dt_synced,
        )
        if detail:
            ret.update(
                summary=self.summary,
                content=self.content,
            )
        return ret


class UserStory(Model):
    class Meta:
        unique_together = ('user', 'story')
        indexes = [
            models.Index(fields=["user", "feed", "story"]),
        ]

    class Admin:
        display_fields = ['user_id', 'feed_id', 'story_id', 'is_readed', 'is_favorited']
        search_fields = ['user_feed_id']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    story = models.ForeignKey(Story, on_delete=models.CASCADE)
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    user_feed = models.ForeignKey(UserFeed, on_delete=models.CASCADE)
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    is_readed = models.BooleanField(default=False)
    dt_readed = models.DateTimeField(**optional, help_text="已读时间")
    is_favorited = models.BooleanField(default=False)
    dt_favorited = models.DateTimeField(**optional, help_text="标星时间")

    @classmethod
    def sync_storys(cls, user_id, user_feed_id=None, limit=200):
        q = UserFeed.objects.filter(user_id=user_id).only('feed_id').distinct()
        if user_feed_id:
            q = q.filter(id=user_feed_id)
        user_feeds = list(q.all())
        feed_ids = {x.feed_id: x.id for x in user_feeds}
        sql = """
        SELECT story.id, story.feed_id
        FROM rssant_api_story AS story
        LEFT OUTER JOIN (
            SELECT id, story_id
            FROM rssant_api_userstory
            WHERE user_id=%s AND feed_id = ANY(%s)
        ) AS userstory
        ON story.id=userstory.story_id
        WHERE story.feed_id = ANY(%s) AND userstory.id IS NULL
        LIMIT %s
        """
        params = [user_id, list(feed_ids), list(feed_ids), limit]
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            new_user_storys = list(cursor.fetchall())
        # batch insert unreaded storys
        bulk_create_storys = []
        with transaction.atomic():
            for story_id, feed_id in new_user_storys:
                user_feed_id = feed_ids[feed_id]
                user_story = UserStory(
                    user_id=user_id, story_id=story_id,
                    feed_id=feed_id, user_feed_id=user_feed_id)
                bulk_create_storys.append(user_story)
            UserStory.objects.bulk_create(bulk_create_storys, batch_size=200)
        return len(new_user_storys)

    def to_dict(self, detail=False):
        ret = self.story.to_dict(detail=detail)
        ret.update(
            id=self.id,
            user=dict(id=self.user_id),
            feed=dict(id=self.user_feed_id),
            dt_created=self.dt_created,
            is_readed=self.is_readed,
            dt_readed=self.dt_readed,
            is_favorited=self.is_favorited,
            dt_favorited=self.dt_favorited
        )
        return ret
