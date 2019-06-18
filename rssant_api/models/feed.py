import gzip

from django.utils import timezone
from django.db import transaction, connection
from cached_property import cached_property

from rssant_common.validator import FeedUnionId
from .errors import FeedExistError, FeedStoryOffsetError, FeedNotFoundError
from .helper import Model, ContentHashMixin, models, optional, JSONField, User, extract_choices


class FeedStatus:
    """
    1. 用户输入URL，直接匹配到已有的Feed，status=ready
    2. 用户输入URL，无匹配, status=pending
       爬虫开始Finder, status=updating
       找到内容，status=ready，没找到, status=error
    3. 定时器扫描，Feed加入队列, status=pending
       爬虫开始抓取, status=updating
       更新内容, status=ready，更新失败 status=error
    """
    PENDING = 'pending'
    UPDATING = 'updating'
    READY = 'ready'
    ERROR = 'error'


FEED_STATUS_CHOICES = extract_choices(FeedStatus)


FEED_DETAIL_FIELDS = [
    'feed__description',
    'feed__encoding',
    'feed__etag',
    'feed__last_modified',
    'feed__content_length',
    'feed__content_hash_base64',
]


class Feed(Model, ContentHashMixin):
    """订阅的最新数据"""
    class Meta:
        indexes = [
            models.Index(fields=["url"]),
        ]

    class Admin:
        display_fields = ['status', 'title', 'url']

    url = models.TextField(unique=True, help_text="供稿地址")
    status = models.CharField(
        max_length=20, choices=FEED_STATUS_CHOICES, default=FeedStatus.PENDING, help_text='状态')
    # RSS解析内容
    title = models.CharField(max_length=200, **optional, help_text="标题")
    link = models.TextField(**optional, help_text="网站链接")
    author = models.CharField(max_length=200, **optional, help_text="作者")
    icon = models.TextField(**optional, help_text="网站Logo或图标")
    description = models.TextField(**optional, help_text="描述或小标题")
    version = models.CharField(max_length=200, **optional, help_text="供稿格式/RSS/Atom")
    dt_updated = models.DateTimeField(help_text="更新时间")
    # RSS抓取相关的状态
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_checked = models.DateTimeField(**optional, help_text="最近一次检查同步时间")
    dt_synced = models.DateTimeField(**optional, help_text="最近一次同步时间")
    encoding = models.CharField(max_length=200, **optional, help_text="编码")
    etag = models.CharField(
        max_length=200, **optional, help_text="HTTP response header ETag")
    last_modified = models.CharField(
        max_length=200, **optional, help_text="HTTP response header Last-Modified")
    content_length = models.IntegerField(
        **optional, help_text='length of content')
    # 其他
    total_storys = models.IntegerField(
        **optional, default=0, help_text="Number of total storys")
    story_publish_period = models.IntegerField(
        **optional, default=30, help_text="story发布周期(天)，按18个月时间窗口计算")
    offset_early_story = models.IntegerField(
        **optional, help_text="最老或18个月前发布的story的offset")
    dt_early_story_published = models.DateTimeField(
        **optional, help_text="最老或18个月前发布的story的发布时间")
    dt_latest_story_published = models.DateTimeField(
        **optional, help_text="最新的story发布时间")

    def to_dict(self, detail=False):
        ret = dict(
            status=self.status,
            url=self.url,
            title=self.title,
            link=self.link,
            author=self.author,
            icon=self.icon,
            version=self.version,
            total_storys=self.total_storys,
            dt_updated=self.dt_updated,
            dt_created=self.dt_created,
            story_publish_period=self.story_publish_period,
            offset_early_story=self.offset_early_story,
            dt_early_story_published=self.dt_early_story_published,
            dt_latest_story_published=self.dt_latest_story_published,
        )
        if detail:
            ret.update(
                description=self.description,
                encoding=self.encoding,
                etag=self.etag,
                last_modified=self.last_modified,
                content_length=self.content_length,
                content_hash_base64=self.content_hash_base64,
                dt_checked=self.dt_checked,
                dt_synced=self.dt_synced,
            )
        return ret

    @staticmethod
    def get_by_pk(feed_id):
        return Feed.objects.get(pk=feed_id)

    @staticmethod
    def get_first_by_url(url):
        return Feed.objects.filter(url=url).first()

    @staticmethod
    def take_outdated(outdate_seconds=300, timeout_seconds=None, limit=100):
        """
        outdate_seconds: 正常检查时间间隔
        timeout_seconds: 异常检查时间间隔
        """
        if not timeout_seconds:
            timeout_seconds = 3 * outdate_seconds
        now = timezone.now()
        dt_outdate_before = now - timezone.timedelta(seconds=outdate_seconds)
        dt_timeout_before = now - timezone.timedelta(seconds=timeout_seconds)
        statuses = [FeedStatus.READY, FeedStatus.ERROR]
        sql_check = """
        SELECT id FROM rssant_api_feed AS feed
        WHERE (status=ANY(%s) AND dt_checked < %s) OR (dt_checked < %s)
        ORDER BY id LIMIT %s
        """
        sql_update_status = """
        UPDATE rssant_api_feed
        SET status=%s, dt_checked=%s
        WHERE id=ANY(%s)
        """
        params = [statuses, dt_outdate_before, dt_timeout_before, limit]
        feed_ids = []
        with connection.cursor() as cursor:
            cursor.execute(sql_check, params)
            for feed_id, in cursor.fetchall():
                feed_ids.append(feed_id)
            cursor.execute(sql_update_status, [FeedStatus.PENDING, now, feed_ids])
        return feed_ids


class RawFeed(Model, ContentHashMixin):
    """订阅的原始数据"""

    class Meta:
        indexes = [
            models.Index(fields=["feed", 'status_code', "dt_created"]),
            models.Index(fields=["url", 'status_code', "dt_created"]),
        ]

    class Admin:
        display_fields = ['feed_id', 'status_code', 'url']

    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    url = models.TextField(help_text="供稿地址")
    encoding = models.CharField(max_length=200, **optional, help_text="编码")
    status_code = models.IntegerField(**optional, help_text='HTTP状态码')
    etag = models.CharField(
        max_length=200, **optional, help_text="HTTP response header ETag")
    last_modified = models.CharField(
        max_length=200, **optional, help_text="HTTP response header Last-Modified")
    headers = JSONField(
        **optional, help_text='HTTP response headers, JSON object')
    is_gzipped = models.BooleanField(
        **optional, default=False, help_text="is content gzip compressed")
    content = models.BinaryField(**optional)
    content_length = models.IntegerField(
        **optional, help_text='length of content')
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")

    def set_content(self, content):
        if content and len(content) >= 1024:
            self.content = gzip.compress(content, compresslevel=9)
            self.is_gzipped = True
        else:
            self.content = content
            self.is_gzipped = False

    def get_content(self, decompress=None):
        if decompress is None:
            decompress = self.is_gzipped
        content = self.content
        if content and decompress:
            content = gzip.decompress(content)
        return content


class UserFeed(Model):
    """用户的订阅状态"""
    class Meta:
        unique_together = ('user', 'feed')
        indexes = [
            models.Index(fields=['user', 'feed']),
        ]

    class Admin:
        display_fields = ['user_id', 'feed_id', 'status', 'url']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE, **optional)
    title = models.CharField(max_length=200, **optional, help_text="用户设置的标题")
    story_offset = models.IntegerField(**optional, default=0, help_text="story offset")
    is_from_bookmark = models.BooleanField(**optional, default=False, help_text='是否从书签导入')
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")

    @property
    def status(self):
        return self.feed.status

    @property
    def url(self):
        return self.feed.url

    @staticmethod
    def get_by_pk(pk, user_id=None, detail=False):
        q = UserFeed.objects.select_related('feed')
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        user_feed = q.get(pk=pk)
        return user_feed


FEED_CREATION_DETAIL_FIELDS = ['message']


class FeedCreation(Model):
    """订阅创建信息"""

    class Meta:
        indexes = [
            models.Index(fields=['user', 'dt_created']),
        ]

    class Admin:
        display_fields = ['user_id', 'feed_id', 'status', 'url', 'is_from_bookmark', 'dt_created']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    feed = models.ForeignKey(Feed, **optional, on_delete=models.CASCADE)
    url = models.TextField(help_text="用户输入的供稿地址")
    is_from_bookmark = models.BooleanField(**optional, default=False, help_text='是否从书签导入')
    status = models.CharField(
        max_length=20, choices=FEED_STATUS_CHOICES, default=FeedStatus.PENDING, help_text='状态')
    message = models.TextField(help_text="查找订阅的日志信息")
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")

    @property
    def is_ready(self):
        return bool(self.feed_id and self.status and self.status == FeedStatus.READY)

    def to_dict(self, detail=False):
        ret = dict(
            id=self.id,
            user=dict(id=self.user_id),
            is_ready=self.is_ready,
            url=self.url,
            is_from_bookmark=self.is_from_bookmark,
            status=self.status,
            dt_created=self.dt_created,
            dt_updated=self.dt_updated,
            feed_unionid=None,
        )
        if self.feed_id:
            feed_unionid = FeedUnionId(self.user_id, self.feed_id)
            ret.update(feed_id=feed_unionid)
        if detail:
            ret.update(message=self.message)
        return ret

    @staticmethod
    def get_by_pk(pk, user_id=None, detail=False):
        q = FeedCreation.objects
        if user_id is not None:
            q = q.filter(user_id=user_id)
        if not detail:
            q = q.defer(*FEED_CREATION_DETAIL_FIELDS)
        return q.get(pk=pk)

    @staticmethod
    def query_by_user(user_id, limit=100, detail=False):
        q = FeedCreation.objects.filter(user_id=user_id)
        if not detail:
            q = q.defer(*FEED_CREATION_DETAIL_FIELDS)
        q = q.order_by('-dt_created')
        if limit:
            q = q[:limit]
        return list(q.all())

    @staticmethod
    def bulk_set_pending(feed_creation_ids):
        q = FeedCreation.objects.filter(id__in=feed_creation_ids)
        return q.update(status=FeedStatus.PENDING, dt_updated=timezone.now())

    @staticmethod
    def delete_by_status(status=None, survival_seconds=None):
        q = FeedCreation.objects
        if status:
            q = q.filter(status=status)
        if survival_seconds:
            deadline = timezone.now() - timezone.timedelta(seconds=survival_seconds)
            q = q.filter(dt_created__lt=deadline)
        num_deleted, __ = q.delete()
        return num_deleted

    @staticmethod
    def query_ids_by_status(status, survival_seconds=None):
        q = FeedCreation.objects.filter(status=status)
        if survival_seconds:
            deadline = timezone.now() - timezone.timedelta(seconds=survival_seconds)
            q = q.filter(dt_created__lt=deadline)
        feed_creation_ids = [x.id for x in q.only('id').all()]
        return feed_creation_ids


class FeedUrlMap(Model):
    """起始 URL 到 Feed URL 直接关联，用于加速FeedFinder"""

    NOT_FOUND = '#'  # 特殊Target

    class Meta:
        indexes = [
            models.Index(fields=["source", "dt_created"]),
        ]

    class Admin:
        display_fields = ['source', 'target', 'dt_created']

    source = models.TextField(help_text="起始地址")
    target = models.TextField(help_text="供稿地址")
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")

    @classmethod
    def find_target(cls, source):
        q = cls.objects.filter(source=source).order_by('-dt_created')
        url_map = q.first()
        if url_map:
            return url_map.target
        return None

    @classmethod
    def find_all_target(cls, source_list):
        sql = """
        SELECT DISTINCT ON (source)
            id, source, target
        FROM rssant_api_feedurlmap
        WHERE source = ANY(%s)
        ORDER BY source, dt_created DESC
        """
        url_map = {}
        items = cls.objects.raw(sql, [list(source_list)])
        for item in items:
            url_map[item.source] = item.target
        return url_map


class FeedCreateResult:
    def __init__(self, *, created_feeds, existed_feeds, feed_creations):
        self.created_feeds = created_feeds
        self.existed_feeds = existed_feeds
        self.feed_creations = feed_creations

    @classmethod
    def empty(cls):
        return cls(created_feeds=[], existed_feeds=[], feed_creations=[])

    @property
    def total(self):
        return self.num_created_feeds + self.num_existed_feeds + self.num_feed_creations

    @property
    def num_created_feeds(self):
        return len(self.created_feeds)

    @property
    def num_existed_feeds(self):
        return len(self.existed_feeds)

    @property
    def num_feed_creations(self):
        return len(self.feed_creations)


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

    @property
    def dt_updated(self):
        if self._user_feed.dt_updated and self._feed.dt_updated:
            if self._user_feed.dt_updated > self._feed.dt_updated:
                return self._user_feed.dt_updated
            else:
                return self._feed.dt_updated
        else:
            return self._user_feed.dt_updated or self._feed.dt_updated

    @property
    def dt_created(self):
        if self._user_feed.dt_created:
            return self._user_feed.dt_created
        return self._feed.dt_created

    @property
    def story_publish_period(self):
        return self._feed.story_publish_period

    @property
    def offset_early_story(self):
        return self._feed.offset_early_story

    @property
    def dt_early_story_published(self):
        return self._feed.dt_early_story_published

    @property
    def dt_latest_story_published(self):
        return self._feed.dt_latest_story_published

    @property
    def description(self):
        return self._feed.description

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
            title=self.title,
            link=self.link,
            author=self.author,
            icon=self.icon,
            version=self.version,
            total_storys=self.total_storys,
            story_offset=self.story_offset,
            num_unread_storys=self.num_unread_storys,
            story_publish_period=self.story_publish_period,
            offset_early_story=self.offset_early_story,
            dt_updated=self.dt_updated,
            dt_created=self.dt_created,
            dt_early_story_published=self.dt_early_story_published,
            dt_latest_story_published=self.dt_latest_story_published,
        )
        if self._detail:
            ret.update(
                description=self.description,
                encoding=self.encoding,
                etag=self.etag,
                last_modified=self.last_modified,
                content_length=self.content_length,
                content_hash_base64=self.content_hash_base64,
                dt_checked=self.dt_checked,
                dt_synced=self.dt_synced,
            )
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
        if not hints:
            q = UserFeed.objects.select_related('feed').filter(user_id=user_id)
            if not detail:
                q = q.defer(*FEED_DETAIL_FIELDS)
            union_feeds = UnionFeed._merge_user_feeds(list(q.all()), detail=detail)
            return len(union_feeds), union_feeds, []
        hints = {x['id'].feed_id: x['dt_updated'] for x in hints}
        q = UserFeed.objects.filter(user_id=user_id).select_related('feed')
        q = q.only("id", 'feed_id', 'feed__dt_updated')
        user_feeds = list(q.all())
        total = len(user_feeds)
        feed_ids = {user_feed.feed_id for user_feed in user_feeds}
        deteted_ids = []
        for feed_id in set(hints) - feed_ids:
            deteted_ids.append(FeedUnionId(user_id, feed_id))
        updates = []
        for user_feed in user_feeds:
            feed_id = user_feed.id
            dt_updated = user_feed.feed.dt_updated
            if feed_id not in hints or not dt_updated:
                updates.append(feed_id)
            elif dt_updated > hints[feed_id]:
                updates.append(feed_id)
        q = UserFeed.objects.select_related('feed')\
            .filter(user_id=user_id, feed_id__in=updates)
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
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

    @staticmethod
    def set_title(feed_unionid, title):
        union_feed = UnionFeed.get_by_id(feed_unionid)
        user_feed = union_feed._user_feed
        user_feed.title = title
        user_feed.dt_updated = timezone.now()
        user_feed.save()
        return union_feed

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
        for user_feed in q.all():
            num_unread = user_feed.feed.total_storys - user_feed.story_offset
            if num_unread > 0:
                user_feed.story_offset = user_feed.feed.total_storys
                updates.append(user_feed)
        with transaction.atomic():
            for user_feed in updates:
                user_feed.save()
        return len(updates)

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
            user_feed.save()
            return UnionFeed(feed, user_feed), None
        else:
            feed_creation = FeedCreation(user_id=user_id, url=url)
            feed_creation.save()
            return None, feed_creation

    @staticmethod
    def create_by_url_s(*, user_id, urls, batch_size=500, is_from_bookmark=False):
        # 批量预查询，减少SQL查询数量，显著提高性能
        if not urls:
            return FeedCreateResult.empty()
        urls = set(urls)
        url_map = {}
        for url, target in FeedUrlMap.find_all_target(urls).items():
            if target == FeedUrlMap.NOT_FOUND:
                urls.discard(url)
            else:
                url_map[url] = target
        found_feeds = list(Feed.objects.filter(url__in=set(url_map.values())).all())
        feed_map = {x.url: x for x in found_feeds}
        q = UserFeed.objects.filter(user_id=user_id, feed__in=found_feeds).all()
        user_feed_map = {x.feed_id: x for x in q.all()}
        # 多个url匹配到同一个feed的情况，user_feed只能保存一个，要根据feed_id去重
        new_user_feed_ids = set()
        new_user_feeds = []
        feed_creations = []
        for url in urls:
            feed = feed_map.get(url_map.get(url))
            if feed:
                if feed.id in user_feed_map:
                    continue
                new_user_feed_ids.add(feed.id)
            else:
                feed_creation = FeedCreation(
                    user_id=user_id, url=url, is_from_bookmark=is_from_bookmark)
                feed_creations.append(feed_creation)
        new_user_feeds = []
        for feed_id in new_user_feed_ids:
            user_feed = UserFeed(user_id=user_id, feed_id=feed_id)
            new_user_feeds.append(user_feed)
        UserFeed.objects.bulk_create(new_user_feeds, batch_size=batch_size)
        FeedCreation.objects.bulk_create(feed_creations, batch_size=batch_size)
        existed_feeds = UnionFeed._merge_user_feeds(user_feed_map.values())
        union_feeds = UnionFeed._merge_user_feeds(new_user_feeds)
        return FeedCreateResult(
            created_feeds=union_feeds,
            existed_feeds=existed_feeds,
            feed_creations=feed_creations,
        )
