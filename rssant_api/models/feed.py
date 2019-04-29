import gzip

from django.utils import timezone
from django.db import transaction, connection

from .exceptions import FeedExistsException
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
    status = models.CharField(
        max_length=20, choices=FEED_STATUS_CHOICES, default=FeedStatus.PENDING, help_text='状态')
    url = models.TextField(help_text="用户输入的供稿地址")
    title = models.CharField(max_length=200, **optional, help_text="用户设置的标题")
    story_offset = models.IntegerField(**optional, default=0, help_text="story offset")
    is_from_bookmark = models.BooleanField(**optional, default=False, help_text='是否从书签导入')
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")

    def to_dict(self, detail=False):
        if self.feed_id:
            ret = self.feed.to_dict(detail=detail)
            num_unread_storys = self.feed.total_storys - self.story_offset
            ret.update(num_unread_storys=num_unread_storys)
            if self.dt_updated and self.feed.dt_updated and self.dt_updated > self.feed.dt_updated:
                ret.update(dt_updated=self.dt_updated)
        else:
            ret = dict(url=self.url, dt_updated=self.dt_updated)
        ret.update(
            id=self.id,
            user=dict(id=self.user_id),
            dt_created=self.dt_created,
            story_offset=self.story_offset,
        )
        if self.title:
            ret.update(title=self.title)
        if self.status and self.status != FeedStatus.READY:
            ret.update(status=self.status)
        return ret

    @property
    def is_ready(self):
        return self.status and self.status == FeedStatus.READY

    @staticmethod
    def get_by_pk(pk, user_id=None, detail=False):
        q = UserFeed.objects.select_related('feed')
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        user_feed = q.get(pk=pk)
        return user_feed

    @staticmethod
    def query_by_pk_s(pks, user_id=None, detail=False):
        q = UserFeed.objects.select_related('feed')
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        user_feeds = q.filter(pk__in=pks)
        return user_feeds

    @staticmethod
    def get_first_by_user_and_feed(user_id, feed_id):
        return UserFeed.objects.filter(user_id=user_id, feed_id=feed_id).first()

    @staticmethod
    def query_by_user(user_id, hints=None, detail=False, show_pending=False):
        """获取用户所有的订阅，支持增量查询

        hints: T.list(T.dict(id=T.int, dt_updated=T.datetime))
        """

        def sort_user_feeds(user_feeds):
            return list(sorted(user_feeds, key=lambda x: (x.dt_updated, x.id), reverse=True))

        q = UserFeed.objects.filter(user_id=user_id).select_related('feed')
        if not hints:
            if not detail:
                q = q.defer(*FEED_DETAIL_FIELDS)
            user_feeds = list(q.all())
            user_feeds = sort_user_feeds(user_feeds)
            return len(user_feeds), user_feeds, []

        hints = {x['id']: x['dt_updated'] for x in hints}
        q = q.only("id", 'feed_id', 'feed__dt_updated')
        user_feeds_map = {}
        for user_feed in q.all():
            user_feeds_map[user_feed.id] = user_feed
        total = len(user_feeds_map)
        deteted_ids = []
        for user_feed_id in hints:
            if user_feed_id not in user_feeds_map:
                deteted_ids.append(user_feed_id)
        updates = []
        for user_feed in user_feeds_map.values():
            dt_updated = user_feed.feed.dt_updated
            if user_feed.id not in hints or not dt_updated:
                updates.append(user_feed.id)
            elif dt_updated > hints[user_feed.id]:
                updates.append(user_feed.id)
        q = UserFeed.objects.filter(user_id=user_id, id__in=updates)
        if not show_pending:
            q = q.exclude(status=FeedStatus.PENDING)
        q = q.select_related('feed')
        if not detail:
            q = q.defer(*FEED_DETAIL_FIELDS)
        user_feeds = list(q.all())
        user_feeds = sort_user_feeds(user_feeds)
        return total, user_feeds, deteted_ids

    @staticmethod
    def create_by_url(url, user_id):
        feed = None
        target_url = FeedUrlMap.find_target(url)
        if target_url:
            feed = Feed.objects.filter(url=target_url).first()
        if feed:
            user_feed = UserFeed.objects.filter(user_id=user_id, feed=feed).first()
            if user_feed:
                raise FeedExistsException('already exists')
            user_feed = UserFeed(user_id=user_id, feed=feed, url=url, status=FeedStatus.READY)
        else:
            user_feed = UserFeed(user_id=user_id, url=url)
        user_feed.save()
        return user_feed

    @staticmethod
    def delete_by_pk(pk, user_id=None):
        user_feed = UserFeed.get_by_pk(pk, user_id=user_id)
        user_feed.delete()

    def update_story_offset(self, offset):
        self.story_offset = offset
        self.dt_updated = timezone.now()
        self.save()

    def update_title(self, title):
        self.title = title
        self.dt_updated = timezone.now()
        self.save()

    @staticmethod
    def set_all_readed_by_user(user_id, ids=None) -> int:
        if ids is not None and not ids:
            return 0
        q = UserFeed.objects.filter(user_id=user_id)
        if ids is not None:
            q = q.filter(id__in=ids)
        q = q.select_related('feed')\
            .only('id', 'story_offset', 'feed_id', 'feed__total_storys')
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
    def create_by_url_s(urls, user_id, batch_size=500, is_from_bookmark=False):
        # 批量预查询，减少SQL查询数量，显著提高性能
        if not urls:
            return []
        url_map = FeedUrlMap.find_all_target(urls)
        feed_map = {}
        found_feeds = Feed.objects.filter(url__in=set(url_map.values())).all()
        for x in found_feeds:
            feed_map[x.url] = x
        user_feed_map = {}
        found_user_feeds = list(UserFeed.objects.filter(
            user_id=user_id, feed__in=found_feeds).all())
        for x in found_user_feeds:
            user_feed_map[x.feed_id] = x
        user_feed_bulk_creates = []
        for url in urls:
            feed = feed_map.get(url_map.get(url))
            if feed:
                if feed.id in user_feed_map:
                    continue
                user_feed = UserFeed(user_id=user_id, feed=feed, url=url, status=FeedStatus.READY)
                user_feed_bulk_creates.append(user_feed)
            else:
                user_feed = UserFeed(user_id=user_id, url=url, is_from_bookmark=is_from_bookmark)
                user_feed_bulk_creates.append(user_feed)
        UserFeed.objects.bulk_create(user_feed_bulk_creates, batch_size=batch_size)
        user_feeds = found_user_feeds + user_feed_bulk_creates
        user_feeds = list(sorted(user_feeds, key=lambda x: x.url))
        return user_feeds

    @staticmethod
    def bulk_set_pending(user_feed_ids):
        sql_update_status = """
        UPDATE rssant_api_userfeed
        SET status=%s, dt_updated=%s
        WHERE id=ANY(%s)
        """
        with connection.cursor() as cursor:
            cursor.execute(sql_update_status, [FeedStatus.PENDING, timezone.now(), user_feed_ids])

    @staticmethod
    def delete_isolated_by_status(status, survival_seconds=None):
        q = UserFeed.objects.filter(status=status, feed_id__isnull=True)
        if survival_seconds:
            deadline = timezone.now() - timezone.timedelta(seconds=survival_seconds)
            q = q.filter(dt_created__lt=deadline)
        num_deleted, __ = q.delete()
        return num_deleted

    @staticmethod
    def query_isolated_ids_by_status(status, survival_seconds=None):
        q = UserFeed.objects.filter(status=status, feed_id__isnull=True)
        if survival_seconds:
            deadline = timezone.now() - timezone.timedelta(seconds=survival_seconds)
            q = q.filter(dt_created__lt=deadline)
        user_feed_ids = [x.id for x in q.only('id').all()]
        return user_feed_ids


class FeedUrlMap(Model):
    """起始 URL 到 Feed URL 直接关联，用于加速FeedFinder"""
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
        items = cls.objects.raw(sql, [source_list])
        for item in items:
            url_map[item.source] = item.target
        return url_map
