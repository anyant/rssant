import gzip

from django.utils import timezone
from django.db import transaction, connection
from cached_property import cached_property
from validr import T

from rssant_common.validator import FeedUnionId
from rssant_common.detail import Detail
from rssant_api.monthly_story_count import MonthlyStoryCount
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
    4. 当更新feed时发生重定向，且新URL对应的feed已存在，则将旧feed合并到新feed，旧feed标记为DISCARD
    """
    PENDING = 'pending'
    UPDATING = 'updating'
    READY = 'ready'
    ERROR = 'error'
    DISCARD = 'discard'


FEED_STATUS_CHOICES = extract_choices(FeedStatus)


FeedDetailSchema = T.detail.fields("""
    icon
    title
    author
    version
    link
    dryness
    freeze_level
    use_proxy
    dt_first_story_published
    dt_latest_story_published
""").extra_fields("""
    description
    warnings
    encoding
    etag
    last_modified
    content_length
    content_hash_base64
    response_status
    dt_checked
    dt_synced
""").default(False)

FEED_DETAIL_FIELDS = [
    f'feed__{x}' for x in Detail.from_schema(False, FeedDetailSchema).exclude_fields
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
    response_status = models.IntegerField(
        **optional, help_text='response status code')
    # 其他
    monthly_story_count_data = models.BinaryField(
        **optional, max_length=514, help_text="monthly story count data")
    dryness = models.IntegerField(
        **optional, default=0, help_text="Dryness of the feed")
    dt_first_story_published = models.DateTimeField(
        **optional, help_text="最老的story发布时间")
    total_storys = models.IntegerField(
        **optional, default=0, help_text="Number of total storys")
    retention_offset = models.IntegerField(
        **optional, default=0, help_text="stale story == offset < retention_offset")
    freeze_level = models.IntegerField(
        **optional, default=1, help_text="freeze level, 1: normal, N: slow down N times")
    use_proxy = models.BooleanField(
        **optional, default=False, help_text="use proxy or not")
    checksum_data = models.BinaryField(
        **optional, max_length=4096, help_text="feed checksum data")
    warnings = models.TextField(
        **optional, help_text="warning messages when processing the feed")
    # Deprecated since v0.3.1
    story_publish_period = models.IntegerField(
        **optional, default=30, help_text="story发布周期(天)，按18个月时间窗口计算")
    # Deprecated since v0.3.1
    offset_early_story = models.IntegerField(
        **optional, help_text="最老或18个月前发布的story的offset")
    # Deprecated since v0.3.1
    dt_early_story_published = models.DateTimeField(
        **optional, help_text="最老或18个月前发布的story的发布时间")
    dt_latest_story_published = models.DateTimeField(
        **optional, help_text="最新的story发布时间")

    def merge(self, other: "Feed"):
        """
        Merge other feed to self by change other's userfeeds' feed_id to self id.
        User stotys are ignored / not handled.
        """
        user_feeds = UserFeed.objects.only('id', 'user_id', 'feed_id')\
            .filter(feed_id__in=(self.id, other.id)).all()
        self_user_ids = set()
        other_user_feeds = []
        for user_feed in user_feeds:
            if user_feed.feed_id == self.id:
                self_user_ids.add(user_feed.user_id)
            else:
                other_user_feeds.append(user_feed)
        updates = []
        for user_feed in other_user_feeds:
            if user_feed.user_id not in self_user_ids:
                user_feed.feed_id = self.id
                updates.append(user_feed)
        UserFeed.objects.bulk_update(updates, ['feed_id'])
        other.status = FeedStatus.DISCARD
        other.save()

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
            dt_first_story_published=self.dt_first_story_published,
            dt_latest_story_published=self.dt_latest_story_published,
        )
        if detail:
            ret.update(
                dryness=self.dryness,
                freeze_level=self.freeze_level,
                use_proxy=self.use_proxy,
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

    @property
    def monthly_story_count(self):
        return MonthlyStoryCount.load(self.monthly_story_count_data)

    @monthly_story_count.setter
    def monthly_story_count(self, value: MonthlyStoryCount):
        if value is None:
            self.monthly_story_count_data = None
            self.dryness = None
        else:
            self.monthly_story_count_data = value.dump()
            self.dryness = value.dryness()

    @staticmethod
    def get_by_pk(feed_id) -> 'Feed':
        return Feed.objects.get(pk=feed_id)

    @staticmethod
    def get_first_by_url(url) -> 'Feed':
        return Feed.objects.filter(url=url).first()

    @staticmethod
    def take_outdated(outdate_seconds=300, timeout_seconds=None, limit=100):
        feeds = Feed.take_outdated_feeds(
            outdate_seconds=outdate_seconds, timeout_seconds=timeout_seconds, limit=limit)
        return [x['feed_id'] for x in feeds]

    @staticmethod
    def take_outdated_feeds(outdate_seconds=300, timeout_seconds=None, limit=100):
        """
        outdate_seconds: 正常检查时间间隔
        timeout_seconds: 异常检查时间间隔
        """
        if not timeout_seconds:
            timeout_seconds = 3 * outdate_seconds
        statuses = [FeedStatus.READY, FeedStatus.ERROR]
        sql_check = """
        SELECT id, url, etag, last_modified, use_proxy, checksum_data
        FROM rssant_api_feed AS feed
        WHERE
            (
                dt_checked IS NULL
            )
            OR
            (
                (freeze_level IS NULL OR freeze_level < 1) AND (
                    (status=ANY(%s) AND NOW() - dt_checked > %s * '1s'::interval)
                    OR
                    (NOW() - dt_checked > %s * '1s'::interval)
                )
            )
            OR
            (
                (status=ANY(%s) AND NOW() - dt_checked > %s * freeze_level * '1s'::interval)
                OR
                (NOW() - dt_checked > %s * freeze_level * '1s'::interval)
            )
        ORDER BY id LIMIT %s
        """
        sql_update_status = """
        UPDATE rssant_api_feed
        SET status=%s, dt_checked=%s
        WHERE id=ANY(%s)
        """
        params = [
            statuses, outdate_seconds, timeout_seconds,
            statuses, outdate_seconds, timeout_seconds, limit
        ]
        feeds = []
        now = timezone.now()
        columns = ['feed_id', 'url', 'etag', 'last_modified', 'use_proxy', 'checksum_data']
        with connection.cursor() as cursor:
            cursor.execute(sql_check, params)
            for row in cursor.fetchall():
                feeds.append(dict(zip(columns, row)))
            feed_ids = [x['feed_id'] for x in feeds]
            cursor.execute(sql_update_status, [FeedStatus.PENDING, now, feed_ids])
        return feeds

    @staticmethod
    def take_retention_feeds(retention=5000, limit=5):
        sql_check = """
        SELECT id, url FROM rssant_api_feed
        WHERE total_storys - retention_offset > %s
        ORDER BY RANDOM() LIMIT %s
        """
        params = [retention, limit]
        with connection.cursor() as cursor:
            cursor.execute(sql_check, params)
            feeds = []
            for feed_id, url in cursor.fetchall():
                feeds.append(dict(feed_id=feed_id, url=url))
            return feeds

    def unfreeze(self):
        self.freeze_level = 1
        self.save()

    @staticmethod
    def refresh_freeze_level():
        """
        冻结策略:
            1. 无人订阅，冻结1个月。有人订阅时解冻。
            2. 创建时间>=7天，且2年无更新，冻结1个月。有更新时解冻。
            3. 创建时间>=7天，且没有任何内容，冻结7天。有更新时解冻。
            4. 其余订阅参照冻结时间表格。
        统计数据:
            - 90%的订阅小于300KB
            - 99%的订阅小于1500KB
            - 资讯新闻(dryness<500)占40%
            - 周更博客(500<dryness<750)占30%
            - 月更博客(dryness>750)占30%
        +------------+----------+------------+----------+
        |   冻结时间  |  300k以下 | 300k~1500k | 1500k以上 |
        +------------+----------+------------+----------+
        |   资讯新闻  |    1H    |     1H     |    3H    |
        |   周更博客  |    1H    |     2H     |    9H    |
        |   月更博客  |    4H    |     8H     |    9H    |
        +------------+----------+------------+----------+
        """
        # https://stackoverflow.com/questions/7869592/how-to-do-an-update-join-in-postgresql
        sql = """
        WITH t AS (
        SELECT
            feed.id AS id,
            CASE
                WHEN (
                    userfeed.id is NULL
                ) THEN 31 * 24
                WHEN (
                    (feed.dt_created <= NOW() - INTERVAL '7 days')
                    and (feed.dt_latest_story_published <= NOW() - INTERVAL '2 years')
                ) THEN 30 * 24
                WHEN (
                    (feed.dt_created <= NOW() - INTERVAL '7 days')
                    and (feed.dt_latest_story_published is NULL and total_storys <= 0)
                ) THEN 7 * 24
                WHEN (
                    feed.content_length >= 1500 * 1024 AND feed.dryness >= 500
                ) THEN 9
                WHEN (
                    feed.content_length >= 1500 * 1024
                ) THEN 3
                WHEN (
                    feed.dryness >= 750 AND feed.content_length >= 300 * 1024
                ) THEN 8
                WHEN (
                    feed.dryness >= 750
                ) THEN 4
                WHEN (
                    feed.dryness >= 500 AND feed.content_length >= 300 * 1024
                ) THEN 2
                ELSE 1
            END AS freeze_level
        FROM rssant_api_feed AS feed
        LEFT OUTER JOIN rssant_api_userfeed AS userfeed
        ON feed.id = userfeed.feed_id
        )
        UPDATE rssant_api_feed AS feed
        SET freeze_level = t.freeze_level
        FROM t
        WHERE feed.id = t.id
        ;
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)


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

    @classmethod
    def query_ids_by_status(cls, status, survival_seconds=None):
        id_urls = cls.query_id_urls_by_status(status, survival_seconds=survival_seconds)
        return [id for (id, url) in id_urls]

    @classmethod
    def query_id_urls_by_status(cls, status, survival_seconds=None):
        q = FeedCreation.objects.filter(status=status)
        if survival_seconds:
            deadline = timezone.now() - timezone.timedelta(seconds=survival_seconds)
            q = q.filter(dt_created__lt=deadline)
        id_urls = [(x.id, x.url) for x in q.only('id', 'url').all()]
        return id_urls


class FeedUrlMap(Model):
    """起始 URL 到 Feed URL 直接关联，用于加速FeedFinder"""

    NOT_FOUND = '#'  # 特殊Target
    NOT_FOUND_TTL = timezone.timedelta(hours=4)
    OK_TTL = timezone.timedelta(days=180)

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
        url_map = cls.find_all_target([source])
        return url_map.get(source)

    @classmethod
    def find_all_target(cls, source_list):
        sql = """
        SELECT DISTINCT ON (source)
            id, source, target
        FROM rssant_api_feedurlmap
        WHERE source=ANY(%s) AND (target!=%s OR dt_created>%s)
        ORDER BY source, dt_created DESC
        """
        dt_ttl = timezone.now() - cls.NOT_FOUND_TTL
        params = [list(source_list), cls.NOT_FOUND, dt_ttl]
        url_map = {}
        items = cls.objects.raw(sql, params)
        for item in items:
            url_map[item.source] = item.target
        return url_map

    @classmethod
    def delete_by_retention(cls, limit=5000):
        sql = """
        DELETE FROM rssant_api_feedurlmap
        WHERE ctid IN (
            SELECT ctid FROM rssant_api_feedurlmap
            WHERE (target=%s AND dt_created<%s) OR (dt_created<%s)
            LIMIT %s
        )
        """
        now = timezone.now()
        dt_not_found_ttl = now - cls.NOT_FOUND_TTL
        dt_ok_ttl = now - cls.OK_TTL
        params = [cls.NOT_FOUND, dt_not_found_ttl, dt_ok_ttl, limit]
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount


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
                feed_creation = FeedCreation(
                    user_id=user_id, url=url, is_from_bookmark=is_from_bookmark)
                feed_creations.append(feed_creation)
        new_user_feeds = []
        for feed_id in new_user_feed_ids:
            user_feed = UserFeed(user_id=user_id, feed=feed_id_map[feed_id])
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
