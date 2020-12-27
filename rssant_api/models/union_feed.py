import typing
from collections import namedtuple

from django.utils import timezone
from django.db import transaction
from cached_property import cached_property

from rssant_common.validator import FeedUnionId
from rssant_common.detail import Detail
from .errors import FeedExistError, FeedStoryOffsetError, FeedNotFoundError
from .feed import UserFeed, Feed, FeedStatus, FeedDetailSchema, FEED_DETAIL_FIELDS
from .feed_creation import FeedCreation, FeedCreateResult, FeedUrlMap


FeedImportItem = namedtuple('FeedImportItem', 'url, title, group')


class UnionFeed:
    def __init__(self, feed, user_feed, detail=False):
        self._feed = feed
        self._user_feed = user_feed
        self._detail = detail

    @cached_property
    def id(self):
        return FeedUnionId(self._user_feed.user_id, self._feed.id)

    @property
    def user_id(self):
        return self._user_feed.user_id

    @property
    def status(self):
        return self._feed.status

    @property
    def is_ready(self):
        return bool(self.status and self.status == FeedStatus.READY)

    @property
    def url(self):
        return self._feed.url

    @property
    def title(self):
        if self._user_feed.title:
            return self._user_feed.title
        return self._feed.title

    @property
    def group(self):
        return self._user_feed.group

    @property
    def link(self):
        return self._feed.link

    @property
    def author(self):
        return self._feed.author

    @property
    def icon(self):
        return self._feed.icon

    @property
    def version(self):
        return self._feed.version

    @property
    def total_storys(self):
        return self._feed.total_storys

    @property
    def story_offset(self):
        return self._user_feed.story_offset

    @property
    def num_unread_storys(self):
        return self._feed.total_storys - self._user_feed.story_offset

    @staticmethod
    def _union_dt_updated(dt_1, dt_2):
        if dt_1 and dt_2:
            return max(dt_1, dt_2)
        else:
            return dt_1 or dt_2

    @property
    def dt_updated(self):
        return self._union_dt_updated(
            self._user_feed.dt_updated, self._feed.dt_updated)

    @property
    def dt_created(self):
        if self._user_feed.dt_created:
            return self._user_feed.dt_created
        return self._feed.dt_created

    @property
    def dryness(self):
        return self._feed.dryness

    @property
    def freeze_level(self):
        return self._feed.freeze_level

    @property
    def use_proxy(self):
        return self._feed.use_proxy

    @property
    def dt_first_story_published(self):
        return self._feed.dt_first_story_published

    @property
    def dt_latest_story_published(self):
        return self._feed.dt_latest_story_published

    @property
    def description(self):
        return self._feed.description

    @property
    def warnings(self) -> str:
        return self._feed.warnings

    @property
    def encoding(self):
        return self._feed.encoding

    @property
    def etag(self):
        return self._feed.etag

    @property
    def last_modified(self):
        return self._feed.last_modified

    @property
    def response_status(self):
        return self._feed.response_status

    @property
    def content_length(self):
        return self._feed.content_length

    @property
    def content_hash_base64(self):
        return self._feed.content_hash_base64

    @property
    def dt_checked(self):
        return self._feed.dt_checked

    @property
    def dt_synced(self):
        return self._feed.dt_synced

    def to_dict(self):
        ret = dict(
            id=self.id,
            user=dict(id=self.user_id),
            is_ready=self.is_ready,
            status=self.status,
            url=self.url,
            total_storys=self.total_storys,
            story_offset=self.story_offset,
            num_unread_storys=self.num_unread_storys,
            dt_updated=self.dt_updated,
            dt_created=self.dt_created,
        )
        detail = Detail.from_schema(self._detail, FeedDetailSchema)
        for k in detail.include_fields:
            ret[k] = getattr(self, k)
        return ret

    @staticmethod
    def get_by_id(feed_unionid, detail=False):
        user_id, feed_id = feed_unionid
        q = UserFeed.objects.select_related('feed')
        q = q.filter(user_id=user_id, feed_id=feed_id)
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
        try:
            user_feed = q.get()
        except UserFeed.DoesNotExist as ex:
            raise FeedNotFoundError(str(ex)) from ex
        return UnionFeed(user_feed.feed, user_feed, detail=detail)

    @staticmethod
    def bulk_delete(feed_ids):
        return Feed.objects.filter(id__in=list(feed_ids)).delete()

    @staticmethod
    def _merge_user_feeds(user_feeds, detail=False):
        def sort_union_feeds(x):
            return (bool(x.dt_updated), x.dt_updated, x.id)
        union_feeds = []
        for user_feed in user_feeds:
            union_feeds.append(UnionFeed(user_feed.feed, user_feed, detail=detail))
        return list(sorted(union_feeds, key=sort_union_feeds, reverse=True))

    @staticmethod
    def query_by_user(user_id, hints=None, detail=False):
        """获取用户所有的订阅，支持增量查询

        hints: T.list(T.dict(id=T.unionid, dt_updated=T.datetime))
        """
        detail = Detail.from_schema(detail, FeedDetailSchema)
        exclude_fields = [f'feed__{x}' for x in detail.exclude_fields]
        if not hints:
            q = UserFeed.objects.select_related('feed').filter(user_id=user_id)
            q = q.defer(*exclude_fields)
            union_feeds = UnionFeed._merge_user_feeds(list(q.all()), detail=detail)
            return len(union_feeds), union_feeds, []
        hints = {x['id'].feed_id: x['dt_updated'] for x in hints}
        q = UserFeed.objects.filter(user_id=user_id).select_related('feed')
        q = q.only("id", 'feed_id', 'dt_updated', 'feed__dt_updated')
        user_feeds = list(q.all())
        total = len(user_feeds)
        feed_ids = {user_feed.feed_id for user_feed in user_feeds}
        deteted_ids = []
        for feed_id in set(hints) - feed_ids:
            deteted_ids.append(FeedUnionId(user_id, feed_id))
        updates = []
        for user_feed in user_feeds:
            feed_id = user_feed.feed_id
            dt_updated = UnionFeed._union_dt_updated(
                user_feed.dt_updated, user_feed.feed.dt_updated)
            if feed_id not in hints or not dt_updated:
                updates.append(feed_id)
            elif dt_updated > hints[feed_id]:
                updates.append(feed_id)
        q = UserFeed.objects.select_related('feed')\
            .filter(user_id=user_id, feed_id__in=updates)
        q = q.defer(*exclude_fields)
        union_feeds = UnionFeed._merge_user_feeds(list(q.all()), detail=detail)
        return total, union_feeds, deteted_ids

    @staticmethod
    def delete_by_id(feed_unionid):
        user_id, feed_id = feed_unionid
        try:
            user_feed = UserFeed.objects.only('id').get(user_id=user_id, feed_id=feed_id)
        except UserFeed.DoesNotExist as ex:
            raise FeedNotFoundError(str(ex)) from ex
        user_feed.delete()

    @staticmethod
    def set_story_offset(feed_unionid, offset):
        union_feed = UnionFeed.get_by_id(feed_unionid)
        if not offset:
            offset = union_feed.total_storys
        if offset > union_feed.total_storys:
            raise FeedStoryOffsetError('offset too large')
        user_feed = union_feed._user_feed
        user_feed.story_offset = offset
        user_feed.dt_updated = timezone.now()
        user_feed.save()
        return union_feed

    @classmethod
    def set_title(cls, feed_unionid, title):
        return cls._set_fields(feed_unionid, title=title)

    @classmethod
    def set_group(cls, feed_unionid, group):
        return cls._set_fields(feed_unionid, group=group)

    @staticmethod
    def _set_fields(feed_unionid, **fields):
        union_feed = UnionFeed.get_by_id(feed_unionid)
        user_feed = union_feed._user_feed
        for key, value in fields.items():
            setattr(user_feed, key, value)
        user_feed.dt_updated = timezone.now()
        user_feed.save()
        return union_feed

    @staticmethod
    def set_all_group(user_id: int, feed_ids: list, *, group: str) -> int:
        q = UserFeed.objects.filter(user_id=user_id).filter(feed_id__in=feed_ids)
        return q.update(
            group=group,
            dt_updated=timezone.now(),
        )

    @staticmethod
    def set_all_readed_by_user(user_id, ids=None) -> int:
        if ids is not None and not ids:
            return 0
        q = UserFeed.objects.select_related('feed').filter(user_id=user_id)
        feed_ids = [x.feed_id for x in ids]
        if ids is not None:
            q = q.filter(feed_id__in=feed_ids)
        q = q.only('_version', 'id', 'story_offset', 'feed_id', 'feed__total_storys')
        updates = []
        now = timezone.now()
        for user_feed in q.all():
            num_unread = user_feed.feed.total_storys - user_feed.story_offset
            if num_unread > 0:
                user_feed.story_offset = user_feed.feed.total_storys
                user_feed.dt_updated = now
                updates.append(user_feed)
        with transaction.atomic():
            for user_feed in updates:
                user_feed.save()
        return len(updates)

    @staticmethod
    def delete_all(user_id, ids=None) -> int:
        if ids is not None and not ids:
            return 0
        q = UserFeed.objects.select_related('feed').filter(user_id=user_id)
        if ids is not None:
            feed_ids = [x.feed_id for x in ids]
            q = q.filter(feed_id__in=feed_ids)
        q = q.only('_version', 'id')
        num_deleted, details = q.delete()
        return num_deleted

    @staticmethod
    def create_by_url(*, user_id, url):
        feed = None
        target = FeedUrlMap.find_target(url)
        if target and target != FeedUrlMap.NOT_FOUND:
            feed = Feed.objects.filter(url=target).first()
        if feed:
            user_feed = UserFeed.objects.filter(user_id=user_id, feed=feed).first()
            if user_feed:
                raise FeedExistError('already exists')
            user_feed = UserFeed(user_id=user_id, feed=feed)
            feed.unfreeze()
            user_feed.save()
            return UnionFeed(feed, user_feed), None
        else:
            feed_creation = FeedCreation(user_id=user_id, url=url)
            feed_creation.save()
            return None, feed_creation

    @staticmethod
    def create_by_imports(
        *,
        user_id: int,
        imports: typing.List[FeedImportItem],
        batch_size: int = 500,
        is_from_bookmark: bool = False,
    ) -> FeedCreateResult:
        # 批量预查询，减少SQL查询数量，显著提高性能
        if not imports:
            return FeedCreateResult.empty()
        import_map = {x.url: x for x in imports}
        urls = set(import_map.keys())
        url_map = {}
        for url, target in FeedUrlMap.find_all_target(urls).items():
            if target == FeedUrlMap.NOT_FOUND:
                urls.discard(url)
            else:
                url_map[url] = target
        # url not existed in url_map: url_map outdated or new url
        for url in (urls - set(url_map.keys())):
            url_map[url] = url
        rev_url_map = {v: k for k, v in url_map.items()}
        found_feeds = list(Feed.objects.filter(url__in=set(url_map.values())).all())
        feed_id_map = {x.id: x for x in found_feeds}
        feed_map = {x.url: x for x in found_feeds}
        q = UserFeed.objects.filter(user_id=user_id, feed__in=found_feeds).all()
        user_feed_map = {x.feed_id: x for x in q.all()}
        for x in user_feed_map.values():
            x.feed = feed_id_map[x.feed_id]
        # 多个url匹配到同一个feed的情况，user_feed只能保存一个，要根据feed_id去重
        new_user_feed_ids = set()
        new_user_feeds = []
        feed_creations = []
        unfreeze_feed_ids = set()
        for url in urls:
            feed = feed_map.get(url_map.get(url))
            if feed:
                if feed.id in user_feed_map:
                    continue
                new_user_feed_ids.add(feed.id)
                if feed.freeze_level and feed.freeze_level > 1:
                    unfreeze_feed_ids.add(feed.id)
            else:
                import_item = import_map.get(url)
                feed_creation = FeedCreation(
                    user_id=user_id, url=url,
                    title=import_item.title if import_item else None,
                    group=import_item.group if import_item else None,
                    is_from_bookmark=is_from_bookmark,
                )
                feed_creations.append(feed_creation)
        new_user_feeds = []
        for feed_id in new_user_feed_ids:
            feed = feed_id_map[feed_id]
            import_item = import_map.get(rev_url_map.get(feed.url))
            # only set UserFeed.title when import title not equal feed title
            title = None
            if import_item and import_item.title and import_item.title != feed.title:
                title = import_item.title
            user_feed = UserFeed(
                user_id=user_id, feed=feed,
                title=title,
                group=import_item.group if import_item else None,
            )
            new_user_feeds.append(user_feed)
        UserFeed.objects.bulk_create(new_user_feeds, batch_size=batch_size)
        FeedCreation.objects.bulk_create(feed_creations, batch_size=batch_size)
        if unfreeze_feed_ids:
            Feed.objects.filter(pk__in=unfreeze_feed_ids).update(freeze_level=1)
        existed_feeds = UnionFeed._merge_user_feeds(user_feed_map.values())
        union_feeds = UnionFeed._merge_user_feeds(new_user_feeds)
        return FeedCreateResult(
            created_feeds=union_feeds,
            existed_feeds=existed_feeds,
            feed_creations=feed_creations,
        )
