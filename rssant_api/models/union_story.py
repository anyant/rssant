from functools import cached_property

from django.db import transaction
from django.utils import timezone

from rssant_common.detail import Detail
from rssant_common.validator import FeedUnionId, StoryUnionId
from rssant_feedlib.processor import story_html_to_text

from .errors import FeedNotFoundError, StoryNotFoundError
from .feed import UserFeed
from .story import (
    USER_STORY_DETAIL_FEILDS,
    Story,
    StoryDetailSchema,
    UserStory,
)
from .story_info import STORY_INFO_DETAIL_FEILDS, StoryId, StoryInfo
from .story_service import STORY_SERVICE

# 旧的数据里有些 summary 是 html 格式，需要转成 text 给页面展示
_DATE_LAST_HTML_SUMMARY = timezone.datetime(2020, 8, 1, tzinfo=timezone.utc)


class UnionStory:
    def __init__(
        self, story, *, user_id, user_feed_id, user_story=None, detail=False,
        story_offset=0, feed_title=None,
    ):
        self._story = story
        self._user_id = user_id
        self._user_feed_id = user_feed_id
        self._user_story = user_story
        self._detail = detail
        self._story_offset = story_offset or 0
        self._feed_title = feed_title

    @cached_property
    def id(self):
        story = self._user_story or self._story
        return StoryUnionId(self._user_id, story.feed_id, story.offset)

    @property
    def user_id(self):
        return self._user_id

    @cached_property
    def feed_id(self):
        story = self._user_story or self._story
        return FeedUnionId(self._user_id, story.feed_id)

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
    def enclosure_urls(self):
        return self._story.enclosure_urls

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
    def is_read(self):
        if self._user_story and self._user_story.is_read:
            return True
        return self.offset < self._story_offset

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
        feed = dict(id=self.feed_id)
        if self._feed_title:
            feed['title'] = self._feed_title
        ret = dict(
            id=self.id,
            user=dict(id=self.user_id),
            feed=feed,
            offset=self.offset,
            title=self.title,
            link=self.link,
            sentence_count=self.sentence_count,
            has_mathjax=self.has_mathjax,
            is_watched=self.is_watched,
            is_read=self.is_read,
            is_favorited=self.is_favorited,
        )
        detail = Detail.from_schema(self._detail, StoryDetailSchema)
        for k in detail.include_fields:
            ret[k] = getattr(self, k)
        return ret

    @staticmethod
    def _check_user_feed_by_story_unionid(story_unionid, only_publish=False):
        user_id, feed_id, offset = story_unionid
        q = UserFeed.objects.select_related('feed').only(
            'id', 'story_offset', 'feed__title',
        ).filter(
            user_id=user_id, feed_id=feed_id
        )
        if only_publish:
            q = q.filter(is_publish=True)
        try:
            user_feed = q.get()
        except UserFeed.DoesNotExist:
            raise StoryNotFoundError()
        return user_feed

    @staticmethod
    def get_by_id(story_unionid, detail=False, only_publish=False):
        user_feed = UnionStory._check_user_feed_by_story_unionid(
            story_unionid, only_publish=only_publish
        )
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
            user_feed_id=user_feed.id,
            user_story=user_story,
            detail=detail,
            story_offset=user_feed.story_offset,
            feed_title=user_feed.feed.title,
        )

    @staticmethod
    def get_by_feed_offset(feed_unionid, offset, detail=False, only_publish=False):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.get_by_id(
            story_unionid, detail=detail, only_publish=only_publish
        )

    @staticmethod
    def _merge_storys(
        storys, user_storys, *, user_id, user_feeds=None, detail=False
    ):
        user_storys_map = {(x.feed_id, x.offset): x for x in user_storys}
        user_storys_map_fallback = {x.story_id: x for x in user_storys}
        if user_feeds:
            user_feeds_map = {x.feed_id: x.id for x in user_feeds}
            story_offsets = {x.feed_id: x.story_offset for x in user_feeds}
            feed_titles = {x.feed_id: x.feed.title for x in user_feeds}
        else:
            user_feeds_map = {x.feed_id: x.user_feed_id for x in user_storys}
            feed_ids = {story.feed_id for story in storys}
            feed_meta = UserFeed.objects.select_related('feed').filter(
                user_id=user_id, feed_id__in=feed_ids
            ).only('feed_id', 'story_offset', 'feed__title')
            story_offsets = {x.feed_id: x.story_offset for x in feed_meta}
            feed_titles = {x.feed_id: x.feed.title for x in feed_meta}
        ret = []
        for story in storys:
            user_story = user_storys_map.get((story.feed_id, story.offset))
            # FIXME: 旧版的story是根据id关联到user_story，由于feed merge问题，特殊处理
            # 但是feed merge之后，新feed的offset可能不一致，还是会有story匹配错误
            if not user_story:
                story_id = getattr(story, 'id', None)
                if story_id is not None:
                    user_story = user_storys_map_fallback.get(story_id)
            user_feed_id = user_feeds_map.get(story.feed_id)
            ret.append(
                UnionStory(
                    story,
                    user_id=user_id,
                    user_feed_id=user_feed_id,
                    user_story=user_story,
                    detail=detail,
                    story_offset=story_offsets.get(story.feed_id, 0),
                    feed_title=feed_titles.get(story.feed_id),
                )
            )
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
        q = StoryInfo.objects.filter(pk__gte=begin_id, pk__lte=end_id)
        if not detail:
            q = q.defer(*STORY_INFO_DETAIL_FEILDS)
        story_info_s = list(q.all())
        storys = [STORY_SERVICE.to_common(x) for x in story_info_s]
        return storys

    @classmethod
    def _query_user_storys_by_offset(cls, user_id, feed_id, offset_s):
        q = UserStory.objects.filter(
            user_id=user_id, feed_id=feed_id, offset__in=offset_s
        )
        q = q.exclude(is_favorited=False, is_watched=False, is_read=False)
        user_storys = list(q.all())
        return user_storys

    @classmethod
    def _query_storys(cls, feed_id, offset, size, detail):
        storys = cls._query_storys_by_story_service(
            feed_id, offset, size, detail=detail
        )
        got_offset_s = set(x.offset for x in storys)
        if len(storys) < size:
            for story in cls._query_storys_by_feed(
                feed_id, offset, size, detail=detail
            ):
                if story.offset not in got_offset_s:
                    storys.append(story)
        storys = list(sorted(storys, key=lambda x: x.offset))
        return storys

    @classmethod
    def query_by_feed(
        cls,
        feed_unionid,
        offset=None,
        size=10,
        detail=False,
        only_publish: bool = False,
    ):
        user_id, feed_id = feed_unionid
        q = UserFeed.objects.select_related('feed')
        q = q.filter(user_id=user_id, feed_id=feed_id)
        if only_publish:
            q = q.filter(is_publish=True)
        q = q.only(
            'id', 'story_offset', 'feed_id', 'feed__id', 'feed__total_storys',
            'feed__retention_offset',
        )
        try:
            user_feed = q.get()
        except UserFeed.DoesNotExist as ex:
            raise FeedNotFoundError() from ex
        total = user_feed.feed.total_storys
        if offset is None:
            offset = user_feed.story_offset
        # 保留策略删掉了窗口之前的文章，从窗口起点开始查，
        # 避免老用户、新订阅从旧 offset 打开看到空页。
        retention_offset = user_feed.feed.retention_offset or 0
        if offset < retention_offset:
            offset = retention_offset
        if offset + size > total:
            size = total - offset
        storys = cls._query_storys(feed_id, offset, size, detail=detail)
        offset_s = [x.offset for x in storys]
        user_storys = cls._query_user_storys_by_offset(
            user_id, feed_id, offset_s
        )
        ret = UnionStory._merge_storys(
            storys,
            user_storys,
            user_feeds=[user_feed],
            user_id=user_id,
            detail=detail,
        )
        return total, offset, ret

    @classmethod
    def query_recent_by_user(
        cls, user_id, feed_unionids=None, days=14, limit=100,
        per_feed_limit=3, unread_only=False, detail=False,
    ):
        """最近新增：按收录时间取时间窗内的文章，每个订阅最多 per_feed_limit 篇。

        Returns: (total, storys)
        """
        if (not feed_unionids) and feed_unionids is not None:
            return 0, []  # when feed_unionids is empty list, return empty list
        since = timezone.now() - timezone.timedelta(days=days)
        q = UserFeed.objects.select_related('feed').filter(
            user_id=user_id, feed__dt_updated__gte=since,
        )
        if feed_unionids:
            feed_ids = [x.feed_id for x in feed_unionids]
            q = q.filter(feed_id__in=feed_ids)
        q = q.only(
            'feed_id', 'story_offset', 'feed__id', 'feed__title',
            'feed__total_storys', 'feed__retention_offset',
        )
        user_feeds = list(q.all())
        if not user_feeds:
            return 0, []
        detail_obj = Detail.from_schema(detail, StoryDetailSchema)
        candidates = []
        for user_feed in user_feeds:
            feed = user_feed.feed
            total = feed.total_storys or 0
            if total <= 0:
                continue
            high = total - 1
            low = max(feed.retention_offset or 0, total - 300)
            if high < low:
                continue
            infos = StoryInfo.objects.filter(
                pk__gte=StoryId.encode(feed.id, low),
                pk__lte=StoryId.encode(feed.id, high),
                dt_created__gte=since,
            ).order_by('-pk')
            if not detail_obj:
                infos = infos.defer(*STORY_INFO_DETAIL_FEILDS)
            candidates.extend(
                (STORY_SERVICE.to_common(info), user_feed) for info in infos
            )
        if not candidates:
            return 0, []
        offsets_by_feed = {}
        for story, user_feed in candidates:
            offsets_by_feed.setdefault(story.feed_id, []).append(story.offset)
        user_storys = []
        for feed_id, offsets in offsets_by_feed.items():
            user_storys.extend(
                UserStory.objects.filter(
                    user_id=user_id, feed_id=feed_id, offset__in=offsets,
                ).exclude(is_favorited=False, is_watched=False, is_read=False)
            )
        read_keys = {
            (x.feed_id, x.offset) for x in user_storys if x.is_read
        }
        picked_by_feed = {}
        for story, user_feed in candidates:
            if unread_only:
                if story.offset < user_feed.story_offset:
                    continue
                if (story.feed_id, story.offset) in read_keys:
                    continue
            picked_by_feed.setdefault(story.feed_id, []).append((story, user_feed))
        picked = []
        for items in picked_by_feed.values():
            items.sort(
                key=lambda x: (x[0].dt_published or x[0].dt_created, x[0].offset),
                reverse=True,
            )
            picked.extend(items[:per_feed_limit])
        total = len(picked)
        picked.sort(
            key=lambda x: (x[0].dt_published or x[0].dt_created, x[0].offset),
            reverse=True,
        )
        picked = picked[:limit]
        union_storys = cls._merge_storys(
            [x[0] for x in picked],
            user_storys,
            user_feeds=user_feeds,
            user_id=user_id,
            detail=detail,
        )
        return total, union_storys

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
            user_id=user_id, feed_id__in=feed_ids, story_id__in=story_ids
        )
        q = q.exclude(is_favorited=False, is_watched=False, is_read=False)
        user_storys = list(q.all())
        union_storys = UnionStory._merge_storys(
            storys, user_storys, user_id=user_id, detail=detail
        )
        return union_storys

    @classmethod
    def _query_union_storys_by_offset(cls, user_id, storys, detail):
        if not storys:
            return []
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
            storys, user_storys, user_id=user_id, detail=detail
        )
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
    def batch_get_by_feed_offset(
        cls, user_id, story_keys, detail=False, content=False,
    ):
        """
        story_keys: List[Tuple[feed_id, offset]]
        content: 是否从正文存储读取 content（正文较重，按需开启）
        """
        story_keys = cls._validate_story_keys(user_id, story_keys)
        if not story_keys:
            return []
        storys = cls._batch_get_story_infos(story_keys, detail=detail)
        finish_story_keys = set((x.feed_id, x.offset) for x in storys)
        remain_story_keys = list(sorted(set(story_keys) - finish_story_keys))
        if remain_story_keys:
            storys.extend(
                Story.batch_get_by_offset(remain_story_keys, detail=detail)
            )
        if content:
            offsets_by_feed = {}
            for story_key in story_keys:
                feed_id, offset = story_key
                offsets_by_feed.setdefault(feed_id, []).append(offset)
            content_map = {}
            for feed_id, offsets in offsets_by_feed.items():
                for story in STORY_SERVICE.batch_get_by_offset(
                    feed_id, offsets, detail=True
                ):
                    content_map[(feed_id, story.offset)] = story.content
            for story in storys:
                story.content = content_map.get((story.feed_id, story.offset))
        union_storys = cls._query_union_storys_by_offset(
            user_id=user_id, storys=storys, detail=detail
        )
        return union_storys

    @staticmethod
    def _query_by_tag(
        user_id, is_favorited=None, is_watched=None, detail=False
    ):
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
        union_storys = UnionStory._merge_storys(
            storys, user_storys, user_id=user_id, detail=detail
        )
        return union_storys

    @staticmethod
    def query_favorited(user_id, detail=False):
        return UnionStory._query_by_tag(
            user_id, is_favorited=True, detail=detail
        )

    @staticmethod
    def query_watched(user_id, detail=False):
        return UnionStory._query_by_tag(
            user_id, is_watched=True, detail=detail
        )

    @staticmethod
    def _set_tag_by_id(story_unionid, is_favorited=None, is_watched=None):
        user_id, feed_id, offset = story_unionid
        story = STORY_SERVICE.set_user_marked(feed_id, offset)
        if not story:
            story = Story.get_by_offset(feed_id, offset, detail=False)
        user_feed = UserFeed.objects.select_related('feed').only(
            'id', 'user_id', 'feed_id', 'story_offset', 'feed__title',
        ).get(
            user_id=user_id, feed_id=feed_id
        )
        user_feed_id = user_feed.id
        try:
            user_story = UserStory.get_by_offset(
                user_id, feed_id, offset, detail=False
            )
        except UserStory.DoesNotExist:
            user_story = None
        with transaction.atomic():
            if user_story is None:
                user_story = UserStory(
                    user_id=user_id,
                    feed_id=feed_id,
                    user_feed_id=user_feed_id,
                    story_id=story.id,
                    offset=offset,
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
            story,
            user_id=user_id,
            user_feed_id=user_feed_id,
            user_story=user_story,
            detail=False,
            story_offset=user_feed.story_offset,
            feed_title=user_feed.feed.title,
        )
        return union_story

    @staticmethod
    def set_favorited_by_id(story_unionid, is_favorited):
        return UnionStory._set_tag_by_id(
            story_unionid, is_favorited=is_favorited
        )

    @staticmethod
    def set_watched_by_id(story_unionid, is_watched):
        return UnionStory._set_tag_by_id(story_unionid, is_watched=is_watched)

    @staticmethod
    def set_favorited_by_feed_offset(feed_unionid, offset, is_favorited):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.set_favorited_by_id(
            story_unionid, is_favorited=is_favorited
        )

    @staticmethod
    def set_watched_by_feed_offset(feed_unionid, offset, is_watched):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory.set_watched_by_id(
            story_unionid, is_watched=is_watched
        )

    @staticmethod
    def _set_read_by_id(story_unionid, is_read=True):
        """标记单篇已读。只写 UserStory.is_read，不碰 is_watched 和 Story.is_user_marked。"""
        user_id, feed_id, offset = story_unionid
        try:
            # 大部分文章只有 StoryInfo，UserStory.story 是必填外键，需要补一条 Story 记录；
            # 不设置 is_user_marked，文章仍然会按 retention 清理。
            story = STORY_SERVICE.get_or_create_story(feed_id, offset)
        except Story.DoesNotExist:
            raise StoryNotFoundError()
        user_feed = UserFeed.objects.select_related('feed').only(
            'id', 'user_id', 'feed_id', 'story_offset', 'feed__title',
        ).get(
            user_id=user_id, feed_id=feed_id
        )
        with transaction.atomic():
            try:
                user_story = UserStory.get_by_offset(
                    user_id, feed_id, offset, detail=False
                )
            except UserStory.DoesNotExist:
                user_story = UserStory(
                    user_id=user_id,
                    feed_id=feed_id,
                    user_feed_id=user_feed.id,
                    story_id=story.id,
                    offset=offset,
                )
            user_story.is_read = is_read
            user_story.save()
        union_story = UnionStory(
            story,
            user_id=user_id,
            user_feed_id=user_feed.id,
            user_story=user_story,
            detail=False,
            story_offset=user_feed.story_offset,
            feed_title=user_feed.feed.title,
        )
        return union_story

    @staticmethod
    def set_read_by_feed_offset(feed_unionid, offset, is_read=True):
        story_unionid = StoryUnionId(*feed_unionid, offset)
        return UnionStory._set_read_by_id(story_unionid, is_read=is_read)
