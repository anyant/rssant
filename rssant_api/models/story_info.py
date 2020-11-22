import typing
from django.utils import timezone
from rssant_common.detail import Detail
from .helper import models, SealableModel, VersionedMixin, VersionField, optional
from .story_storage import StoryId
from .story import STORY_DETAIL_FEILDS, StoryDetailSchema


STORY_INFO_DETAIL_FEILDS = list(STORY_DETAIL_FEILDS)
STORY_INFO_DETAIL_FEILDS.remove('content')


class StoryInfo(VersionedMixin, SealableModel):

    class Admin:
        display_fields = ['id', 'title', 'link']

    _version = VersionField()

    id = models.BigIntegerField(primary_key=True, help_text='feed_id and offset')
    unique_id = models.CharField(max_length=200, help_text="Unique ID")
    title = models.CharField(max_length=200, help_text="标题")
    link = models.TextField(help_text="文章链接")
    author = models.CharField(max_length=200, **optional, help_text='作者')
    image_url = models.TextField(**optional, help_text="图片链接")
    audio_url = models.TextField(**optional, help_text="播客音频链接")
    iframe_url = models.TextField(**optional, help_text="视频iframe链接")
    has_mathjax = models.BooleanField(**optional, help_text='has MathJax')
    is_user_marked = models.BooleanField(
        **optional, help_text='is user favorited or watched ever')
    dt_published = models.DateTimeField(help_text="发布时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")
    # fix auto_now_add not support modify. see also: DateField.auto_now_add
    dt_created = models.DateTimeField(default=timezone.now, help_text="创建时间")
    dt_synced = models.DateTimeField(**optional, help_text="最近一次同步时间")
    content_length = models.IntegerField(**optional, help_text='content length')
    summary = models.TextField(**optional, help_text="摘要或较短的内容")
    content_hash_base64 = models.CharField(
        max_length=200, **optional, help_text='base64 hash value of content')
    sentence_count = models.IntegerField(**optional, help_text='sentence count')

    @property
    def feed_id(self) -> int:
        feed_id, offset = StoryId.decode(self.id)
        return feed_id

    @property
    def offset(self) -> int:
        feed_id, offset = StoryId.decode(self.id)
        return offset

    @classmethod
    def _get_exclude_fields(cls, detail):
        detail = Detail.from_schema(detail, StoryDetailSchema)
        exclude_fields = set(detail.exclude_fields)
        exclude_fields.discard('content')
        return exclude_fields

    @classmethod
    def get(cls, feed_id, offset, detail=False) -> "StoryInfo":
        q = StoryInfo.objects.filter(pk=StoryId.encode(feed_id, offset))
        q = q.defer(*cls._get_exclude_fields(detail))
        return q.seal().first()

    @classmethod
    def batch_get(cls, keys, detail=False) -> typing.List["StoryInfo"]:
        if not keys:
            return []
        story_ids = []
        for feed_id, offset in keys:
            story_ids.append(StoryId.encode(feed_id, offset))
        q = StoryInfo.objects.filter(pk__in=story_ids)
        q = q.defer(*cls._get_exclude_fields(detail))
        return list(q.seal().all())

    @staticmethod
    def delete_by_retention_offset(feed_id, retention_offset) -> int:
        """
        delete storys < retention_offset and not is_user_marked
        """
        begin_story_id = StoryId.encode(feed_id, 0)
        retention_story_id = StoryId.encode(feed_id, retention_offset)
        n, __ = StoryInfo.objects\
            .filter(pk__gte=begin_story_id, pk__lt=retention_story_id)\
            .exclude(is_user_marked=True)\
            .delete()
        return n
