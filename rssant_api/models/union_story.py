from django.utils import timezone
from django.db import transaction
from functools import cached_property

from rssant_feedlib.processor import story_html_to_text
from rssant_common.validator import StoryUnionId, FeedUnionId
from rssant_common.detail import Detail
from .feed import UserFeed
from .story import Story, UserStory, StoryDetailSchema, USER_STORY_DETAIL_FEILDS
from .story_info import StoryInfo, StoryId, STORY_INFO_DETAIL_FEILDS
from .errors import FeedNotFoundError, StoryNotFoundError
from .story_service import STORY_SERVICE


# 旧的数据里有些 summary 是 html 格式，需要转成 text 给页面展示
_DATE_LAST_HTML_SUMMARY = timezone.datetime(2020, 8, 1, tzinfo=timezone.utc)


class UnionStory:

    def __init__(self, story, *, user_id, user_feed_id, user_story=None, detail=False):
        self._story = story
        self._user_id = user_id
        self._user_feed_id = user_feed_id
        self._user_story = user_story
        self._detail = detail

    @cached_property
    def id(self):
        return StoryUnionId(self._user_id, self._story.feed_id, self._story.offset)

    @property
    def user_id(self):
        return self._user_id

    @cached_property
    def feed_id(self):
        return FeedUnionId(self._user_id, self._story.feed_id)

    @property
    def offset(self):
        return self._story.offset

    @property
    def unique_id(self):
        return self._story.unique_id

    @property
    def title(self):
        return self._story.title

    @property
    def link(self):
        return self._story.link

    @property
    def author(self):
        return self._story.author

    @property
    def image_url(self):
        return self._story.image_url

    @property
    def iframe_url(self):
        return self._story.iframe_url

    @property
    def audio_url(self):
        return self._story.audio_url

    @property
    def has_mathjax(self):
        return self._story.has_mathjax

    @property
    def sentence_count(self) -> int:
        # only StoryInfo support sentence_count field
        return getattr(self._story, 'sentence_count', None)

    @property
    def dt_published(self):
        return self._story.dt_published

    @property
    def dt_updated(self):
        return self._story.dt_updated

    @property
    def dt_created(self):
        return self._story.dt_created

    @property
    def dt_synced(self):
        return self._story.dt_synced

    @property
    def is_watched(self):
        if not self._user_story:
            return False
        return self._user_story.is_watched

    @property
    def dt_watched(self):
        if not self._user_story:
            return None
        return self._user_story.dt_watched

    @property
    def is_favorited(self):
        if not self._user_story:
            return False
        return self._user_story.is_favorited

    @property
    def dt_favorited(self):
        if not self._user_story:
            return None
        return self._user_story.dt_favorited

    @property
    def content_hash_base64(self):
        return self._story.content_hash_base64

    @cached_property
    def summary(self):
        if self.dt_created < _DATE_LAST_HTML_SUMMARY:
            return story_html_to_text(self._story.summary)
        return self._story.summary

    @property
    def content(self):
        return self._story.content

    def to_dict(self):
        ret = dict(
            id=self.id,
            user=dict(id=self.user_id),
            feed=dict(id=self.feed_id),
            offset=self.offset,
            title=self.title,
            link=self.link,
            sentence_count=self.sentence_count,
            has_mathjax=self.has_mathjax,
            is_watched=self.is_watched,
            is_favorited=self.is_favorited,
        )
        detail = Detail.from_schema(self._detail, StoryDetailSchema)
        for k in detail.include_fields:
            ret[k] = getattr(self, k)
        return ret

    @staticmethod
    def _check_user_feed_by_story_unionid(story_unionid):
        user_id, feed_id, offset = story_unionid
        q = UserFeed.objects.only('id').filter(user_id=user_id, feed_id=feed_id)
        try:
            user_feed = q.get()
        except UserFeed.DoesNotExist:
            raise StoryNotFoundError()
        return user_feed.id

    @staticmethod
    def get_by_id(story_unionid, detail=False):
        user_feed_id = UnionStory._check_user_feed_by_story_unionid(story_unionid)
        user_id, feed_id, offset = story_unionid
        q = UserStory.objects.select_related('story')
        q = q.filter(user_id=user_id, feed_id=feed_id, offset=offset)
        if not detail:
            q = q.defer(*USER_STORY_DETAIL_FEILDS)
        try:
            user_story = q.get()
        except UserStory.DoesNotExist:
            user_story = None
            story = STORY_SERVICE.get_by_offset(feed_id, offset, detail=detail)
            if not story:
                raise StoryNotFoundError()
        else:
            story = user_story.story
        return UnionStory(
            story,
            user_id=user_id,
            user_feed_id=user_feed_id,
            user_story=user_story,
            detail=detail
        )

    @staticmethod
    def get_by_feed_offset(feed_unionid, offset, detail=False):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.get_by_id(story_unionid, detail=detail)

    @staticmethod
    def _merge_storys(storys, user_storys, *, user_id, user_feeds=None, detail=False):
        user_storys_map = {(x.feed_id, x.offset): x for x in user_storys}
        if user_feeds:
            user_feeds_map = {x.feed_id: x.id for x in user_feeds}
        else:
            user_feeds_map = {x.feed_id: x.user_feed_id for x in user_storys}
        ret = []
        for story in storys:
            user_story = user_storys_map.get((story.feed_id, story.offset))
            user_feed_id = user_feeds_map.get(story.feed_id)
            ret.append(UnionStory(
                story,
                user_id=user_id,
                user_feed_id=user_feed_id,
                user_story=user_story,
                detail=detail
            ))
        return ret

    @classmethod
    def _query_storys_by_feed(cls, feed_id, offset, size, detail):
        q = Story.objects.filter(feed_id=feed_id, offset__gte=offset)
        detail = Detail.from_schema(detail, StoryDetailSchema)
        q = q.defer(*detail.exclude_fields)
        q = q.order_by('offset')[:size]
        storys = list(q.all())
        return storys

    @classmethod
    def _query_storys_by_story_service(cls, feed_id, offset, size, detail):
        begin_id = StoryId.encode(feed_id, offset)
        end_id = StoryId.encode(feed_id, offset + size - 1)
        q = StoryInfo.objects\
            .filter(pk__gte=begin_id, pk__lte=end_id)
        if not detail:
            q = q.defer(*STORY_INFO_DETAIL_FEILDS)
        story_info_s = list(q.all())
        storys = [STORY_SERVICE.to_common(x) for x in story_info_s]
        return storys

    @classmethod
    def _query_user_storys_by_offset(cls, user_id, feed_id, offset_s):
        q = UserStory.objects.filter(user_id=user_id, feed_id=feed_id, offset__in=offset_s)
        q = q.exclude(is_favorited=False, is_watched=False)
        user_storys = list(q.all())
        return user_storys

    @classmethod
    def _query_storys(cls, feed_id, offset, size, detail):
        storys = cls._query_storys_by_story_service(feed_id, offset, size, detail=detail)
        got_offset_s = set(x.offset for x in storys)
        if len(storys) < size:
            for story in cls._query_storys_by_feed(feed_id, offset, size, detail=detail):
                if story.offset not in got_offset_s:
                    storys.append(story)
        storys = list(sorted(storys, key=lambda x: x.offset))
        return storys

    @classmethod
    def query_by_feed(cls, feed_unionid, offset=None, size=10, detail=False):
        user_id, feed_id = feed_unionid
        q = UserFeed.objects.select_related('feed')\
            .filter(user_id=user_id, feed_id=feed_id)\
            .only('id', 'story_offset', 'feed_id', 'feed__id', 'feed__total_storys')
        try:
            user_feed = q.get()
        except UserFeed.DoesNotExist as ex:
            raise FeedNotFoundError() from ex
        total = user_feed.feed.total_storys
        if offset is None:
            offset = user_feed.story_offset
        if offset + size > total:
            size = total - offset
        storys = cls._query_storys(feed_id, offset, size, detail=detail)
        offset_s = [x.offset for x in storys]
        user_storys = cls._query_user_storys_by_offset(user_id, feed_id, offset_s)
        ret = UnionStory._merge_storys(
            storys, user_storys, user_feeds=[user_feed], user_id=user_id, detail=detail)
        return total, offset, ret

    @classmethod
    def query_recent_by_user(cls, user_id, feed_unionids=None, days=14, limit=300, detail=False):
        """
        Deprecated since 1.4.2, use batch_get_by_feed_offset instead
        """
        if (not feed_unionids) and feed_unionids is not None:
            return []  # when feed_unionids is empty list, return empty list
        if feed_unionids:
            feed_ids = [x.feed_id for x in feed_unionids]
            feed_ids = cls._query_user_feed_ids(user_id, feed_ids)
        else:
            feed_ids = cls._query_user_feed_ids(user_id)
        dt_begin = timezone.now() - timezone.timedelta(days=days)
        q = Story.objects.filter(feed_id__in=feed_ids)\
            .filter(dt_published__gte=dt_begin)
        detail = Detail.from_schema(detail, StoryDetailSchema)
        q = q.defer(*detail.exclude_fields)
        q = q.order_by('-dt_published')[:limit]
        storys = list(q.all())
        union_storys = cls._query_union_storys(
            user_id=user_id, storys=storys, detail=detail)
        return union_storys

    @classmethod
    def _query_user_feed_ids(cls, user_id, feed_ids=None):
        q = UserFeed.objects.only('id', 'feed_id')
        if feed_ids is None:
            q = q.filter(user_id=user_id)
        else:
            q = q.filter(user_id=user_id, feed_id__in=feed_ids)
        user_feeds = list(q.all())
        feed_ids = [x.feed_id for x in user_feeds]
        return feed_ids

    @classmethod
    def _query_union_storys(cls, user_id, storys, detail):
        """
        Deprecated since 1.5.0
        """
        story_ids = [x.id for x in storys]
        feed_ids = list(set([x.feed_id for x in storys]))
        q = UserStory.objects.filter(
            user_id=user_id, feed_id__in=feed_ids, story_id__in=story_ids)
        q = q.exclude(is_favorited=False, is_watched=False)
        user_storys = list(q.all())
        union_storys = UnionStory._merge_storys(
            storys, user_storys, user_id=user_id, detail=detail)
        return union_storys

    @classmethod
    def _query_union_storys_by_offset(cls, user_id, storys, detail):
        where_items = []
        for story in storys:
            # ensure integer, avoid sql inject attack
            feed_id, offset = int(story.feed_id), int(story.offset)
            where_items.append(f'("feed_id"={feed_id} AND "offset"={offset})')
        where_clause = ' OR '.join(where_items)
        sql = f"""
        SELECT * FROM rssant_api_userstory
        WHERE user_id=%s AND ({where_clause})
        """
        user_storys = list(UserStory.objects.raw(sql, [user_id]))
        union_storys = UnionStory._merge_storys(
            storys, user_storys, user_id=user_id, detail=detail)
        return union_storys

    @classmethod
    def _validate_story_keys(cls, user_id, story_keys):
        if not story_keys:
            return []
        # verify feed_id is subscribed by user
        feed_ids = list(set(x[0] for x in story_keys))
        feed_ids = set(cls._query_user_feed_ids(user_id, feed_ids))
        verified_story_keys = []
        for feed_id, offset in story_keys:
            if feed_id in feed_ids:
                verified_story_keys.append((feed_id, offset))
        verified_story_keys = list(sorted(verified_story_keys))
        return verified_story_keys

    @classmethod
    def _batch_get_story_infos(cls, story_keys, detail):
        story_info_s = StoryInfo.batch_get(story_keys, detail=detail)
        storys = [STORY_SERVICE.to_common(x) for x in story_info_s]
        return storys

    @classmethod
    def batch_get_by_feed_offset(cls, user_id, story_keys, detail=False):
        """
        story_keys: List[Tuple[feed_id, offset]]
        """
        story_keys = cls._validate_story_keys(user_id, story_keys)
        if not story_keys:
            return []
        storys = cls._batch_get_story_infos(story_keys, detail=detail)
        finish_story_keys = set((x.feed_id, x.offset) for x in storys)
        remain_story_keys = list(sorted(set(story_keys) - finish_story_keys))
        if remain_story_keys:
            storys.extend(Story.batch_get_by_offset(remain_story_keys, detail=detail))
        union_storys = cls._query_union_storys_by_offset(
            user_id=user_id, storys=storys, detail=detail)
        return union_storys

    @staticmethod
    def _query_by_tag(user_id, is_favorited=None, is_watched=None, detail=False):
        q = UserStory.objects.select_related('story').filter(user_id=user_id)
        detail = Detail.from_schema(detail, StoryDetailSchema)
        exclude_fields = [f'story__{x}' for x in detail.exclude_fields]
        q = q.defer(*exclude_fields)
        if is_favorited is not None:
            q = q.filter(is_favorited=is_favorited)
        if is_watched is not None:
            q = q.filter(is_watched=is_watched)
        user_storys = list(q.all())
        storys = [x.story for x in user_storys]
        union_storys = UnionStory._merge_storys(storys, user_storys, user_id=user_id, detail=detail)
        return union_storys

    @staticmethod
    def query_favorited(user_id, detail=False):
        return UnionStory._query_by_tag(user_id, is_favorited=True, detail=detail)

    @staticmethod
    def query_watched(user_id, detail=False):
        return UnionStory._query_by_tag(user_id, is_watched=True, detail=detail)

    @staticmethod
    def _set_tag_by_id(story_unionid, is_favorited=None, is_watched=None):
        user_id, feed_id, offset = story_unionid
        story = STORY_SERVICE.set_user_marked(feed_id, offset)
        if not story:
            story = Story.get_by_offset(feed_id, offset, detail=False)
        user_feed = UserFeed.objects\
            .only('id', 'user_id', 'feed_id')\
            .get(user_id=user_id, feed_id=feed_id)
        user_feed_id = user_feed.id
        try:
            user_story = UserStory.get_by_offset(user_id, feed_id, offset, detail=False)
        except UserStory.DoesNotExist:
            user_story = None
        with transaction.atomic():
            if user_story is None:
                user_story = UserStory(
                    user_id=user_id,
                    feed_id=feed_id,
                    user_feed_id=user_feed_id,
                    story_id=story.id,
                    offset=offset
                )
            if is_favorited is not None:
                user_story.is_favorited = is_favorited
                user_story.dt_favorited = timezone.now()
            if is_watched is not None:
                user_story.is_watched = is_watched
                user_story.dt_watched = timezone.now()
            user_story.save()
            if is_favorited or is_watched:
                if not story.is_user_marked:
                    story.is_user_marked = True
                    story.save()
        union_story = UnionStory(
            story, user_id=user_id, user_feed_id=user_feed_id,
            user_story=user_story, detail=False)
        return union_story

    @staticmethod
    def set_favorited_by_id(story_unionid, is_favorited):
        return UnionStory._set_tag_by_id(story_unionid, is_favorited=is_favorited)

    @staticmethod
    def set_watched_by_id(story_unionid, is_watched):
        return UnionStory._set_tag_by_id(story_unionid, is_watched=is_watched)

    @staticmethod
    def set_favorited_by_feed_offset(feed_unionid, offset, is_favorited):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.set_favorited_by_id(story_unionid, is_favorited=is_favorited)

    @staticmethod
    def set_watched_by_feed_offset(feed_unionid, offset, is_watched):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.set_watched_by_id(story_unionid, is_watched=is_watched)
