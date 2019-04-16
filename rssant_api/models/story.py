from django.utils import timezone

from .helper import Model, ContentHashMixin, models, optional, User
from .feed import Feed, UserFeed


STORY_DETAIL_FEILDS = ['summary', 'content']
USER_STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


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
        display_fields = ['feed_id', 'title', 'link']

    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    offset = models.IntegerField(help_text="Story在Feed中的位置")
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
            offset=self.offset,
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
        display_fields = ['user_id', 'feed_id', 'story_id', 'is_watched', 'is_favorited']
        search_fields = ['user_feed_id']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    story = models.ForeignKey(Story, on_delete=models.CASCADE)
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    user_feed = models.ForeignKey(UserFeed, on_delete=models.CASCADE)
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    is_watched = models.BooleanField(default=False)
    dt_watched = models.DateTimeField(**optional, help_text="关注时间")
    is_favorited = models.BooleanField(default=False)
    dt_favorited = models.DateTimeField(**optional, help_text="标星时间")

    def to_dict(self, detail=False):
        ret = self.story.to_dict(detail=detail)
        ret.update(
            id=self.id,
            user=dict(id=self.user_id),
            feed=dict(id=self.user_feed_id),
            dt_created=self.dt_created,
            is_watched=self.is_watched,
            dt_readed=self.dt_readed,
            is_favorited=self.is_favorited,
            dt_favorited=self.dt_favorited
        )
        return ret

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
    def get_by_feed_offset(user_feed_id, offset, user_id=None, detail=False):
        q = UserStory.objects.select_related('story')
        if not detail:
            q = q.defer(*USER_STORY_DETAIL_FEILDS)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        user_story = q.get(user_feed_id=user_feed_id, story__offset=offset)
        return user_story

    def update_watched(self, is_watched):
        self.is_watched = is_watched
        if is_watched:
            self.dt_watched = timezone.now()
        self.save()

    def update_favorited(self, is_favorited):
        self.is_favorited = is_favorited
        if is_favorited:
            self.dt_watched = timezone.now()
        self.save()

    @staticmethod
    def query_storys_by_feed(user_feed_id, offset=None, user_id=None, size=10, detail=False):
        user_feed = UserFeed.get_by_pk(user_feed_id, user_id=user_id)
        feed_id = user_feed.feed.id
        total = user_feed.feed.total_storys
        if offset is None:
            offset = user_feed.story_offset
        q = Story.objects
        if not detail:
            q = q.defer(*STORY_DETAIL_FEILDS)
        q = q.filter(feed_id=feed_id, offset__gte=offset)
        q = q.order_by('offset')[:size]
        storys = list(q.all())
        return total, offset, storys

    @staticmethod
    def query_by_user(user_id, is_watched=None, is_favorited=None, detail=False):
        q = UserStory.objects.filter(user_id=user_id)
        if is_watched is not None:
            q = q.filter(is_watched=is_watched)
        if is_favorited is not None:
            q = q.filter(is_favorited=is_favorited)
        if not detail:
            q = q.defer(*USER_STORY_DETAIL_FEILDS)
        user_storys = list(q.order_by('-dt_created').all())
        return user_storys
