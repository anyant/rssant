import gzip

from django.utils import timezone
from django.db import connection
from validr import T

from rssant_common.detail import Detail
from rssant_api.monthly_story_count import MonthlyStoryCount
from rssant_api.helper import DuplicateFeedDetector
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
    title
    group,
    dryness
    response_status
    freeze_level
    use_proxy
    dt_first_story_published
    dt_latest_story_published
""").extra_fields("""
    icon
    author
    version
    link
    description
    warnings
    encoding
    etag
    last_modified
    content_length
    content_hash_base64
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
            models.Index(fields=["reverse_url"]),
        ]

    class Admin:
        display_fields = ['status', 'title', 'url']

    # TODO: deprecate url, use reverse_url instead
    url = models.TextField(unique=True, help_text="供稿地址")
    # TODO: make reverse_url unique and not null
    reverse_url = models.TextField(**optional, help_text="倒转URL")
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
        user_feeds = UserFeed.objects.only('id', 'user_id', 'feed_id', 'story_offset')\
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
            user_feed: UserFeed
            if user_feed.user_id not in self_user_ids:
                user_feed.feed_id = self.id
                if user_feed.story_offset > self.total_storys:
                    user_feed.story_offset = self.total_storys
                updates.append(user_feed)
        UserFeed.objects.bulk_update(updates, ['feed_id', 'story_offset'])
        other.status = FeedStatus.DISCARD
        other.save()

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
        sql_check = f"""
        SELECT id, url, etag, last_modified, use_proxy, checksum_data
        FROM rssant_api_feed AS feed
        WHERE
            (status != '{FeedStatus.DISCARD}') AND (
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
            ))
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

    @classmethod
    def _query_feeds_by_reverse_url(cls, begin=None, limit=1000) -> list:
        if begin:
            where = 'AND reverse_url >= %s'
            params = [begin, limit]
        else:
            where = ''
            params = [limit]
        sql = f"""
        SELECT id, reverse_url FROM rssant_api_feed
        WHERE status != '{FeedStatus.DISCARD}' AND
            reverse_url IS NOT NULL AND reverse_url != '' {where}
        ORDER BY reverse_url LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            feeds = list(cursor.fetchall())
        return feeds

    @classmethod
    def find_duplicate_feeds(cls, checkpoint=None, limit=5000):
        """
        find duplicate feeds

        Returns: (duplicates, checkpoint)
            duplicates:
                (primary_feed_id, duplicate_feed_id ...)
                (primary_feed_id, duplicate_feed_id ...)
                ...
        """
        detector = DuplicateFeedDetector()
        feeds = cls._query_feeds_by_reverse_url(begin=checkpoint, limit=limit)
        for feed_id, rev_url in feeds:
            detector.push(feed_id, rev_url)
        if len(feeds) < limit:
            detector.flush()
        next_checkpoint = detector.checkpoint
        # force flush when single host has too many feeds
        if checkpoint is not None and checkpoint == next_checkpoint:
            detector.flush()
            next_checkpoint = rev_url
        got = detector.poll()
        return got, next_checkpoint

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

    @classmethod
    def unfreeze_by_id(cls, feed_id: int):
        Feed.objects.filter(pk=feed_id).update(freeze_level=1)

    @staticmethod
    def refresh_freeze_level():
        """
        活跃用户: 90天内有阅读记录
        冻结策略:
            1. 无人订阅，冻结1个月。有人订阅时解冻。
            2. 创建时间>=7天，且2年无更新，冻结1个月。有更新时解冻。
            3. 创建时间>=7天，且没有任何内容，冻结7天。有更新时解冻。
            4. 无活跃用户订阅，冻结3天。有活跃用户订阅时解冻。
            5. 其余订阅参照冻结时间表格。
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
        sql = f"""
        WITH t AS (
        SELECT
            feed.id AS id,
            CASE
                WHEN (
                    feed_stat.feed_id is NULL OR feed_stat.user_count <= 0
                ) THEN 31 * 24
                WHEN (
                    (feed.dt_created <= NOW() - INTERVAL '7 days')
                    AND (feed.dt_latest_story_published <= NOW() - INTERVAL '2 years')
                ) THEN 30 * 24
                WHEN (
                    (feed.dt_created <= NOW() - INTERVAL '7 days')
                    AND (feed.dt_latest_story_published is NULL and total_storys <= 0)
                ) THEN 7 * 24
                WHEN (
                    feed_stat.active_user_count <= 0
                ) THEN 3 * 24
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
        LEFT OUTER JOIN (
            SELECT
                feed_id,
                COUNT(1) AS user_count,
                SUM(user_stat.is_active) AS active_user_count
            FROM rssant_api_userfeed JOIN (
                SELECT user_id, CASE WHEN (
                    MAX(dt_updated) >= NOW() - INTERVAL '90 days'
                ) THEN 1 ELSE 0 END AS is_active
                FROM rssant_api_userfeed GROUP BY user_id
            ) user_stat
            ON rssant_api_userfeed.user_id = user_stat.user_id
            GROUP BY feed_id
        ) AS feed_stat
        ON feed.id = feed_stat.feed_id
        WHERE feed.status != '{FeedStatus.DISCARD}'
        )
        UPDATE rssant_api_feed AS feed
        SET freeze_level = t.freeze_level
        FROM t
        WHERE feed.id = t.id AND feed.freeze_level != t.freeze_level
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
    group = models.CharField(max_length=200, **optional, help_text="用户设置的分组")
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
