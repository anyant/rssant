from django.utils import timezone
from django.db import connection

from rssant_common.validator import FeedUnionId
from .helper import User, Model, models, optional
from .feed import Feed, FeedStatus, FEED_STATUS_CHOICES


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
    title = models.CharField(max_length=200, **optional, help_text="用户设置的标题")
    group = models.CharField(max_length=200, **optional, help_text="用户设置的分组")
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
            title=self.title,
            group=self.group,
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
    NOT_FOUND_TTL = timezone.timedelta(minutes=3)
    # TODO: retention of OK url maps
    OK_TTL = timezone.timedelta(days=100 * 365)

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
