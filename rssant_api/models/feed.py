import base64
import hashlib

from .helper import Model, models, optional, JSONField, User, extract_choices


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


class ContentHashMixin(models.Model):

    class Meta:
        abstract = True

    content_hash_method = models.CharField(
        max_length=20, **optional, help_text='hash method of content')
    content_hash_value = models.BinaryField(
        max_length=200, **optional, help_text='hash value of content')

    def compute_content_hash(self, content, method=None):
        if not method:
            method = self.content_hash_method
        if not method:
            method = 'sha1'
        h = hashlib.new(method)
        h.update(content)
        return method, h.digest()

    @property
    def content_hash_value_base64(self):
        if not self.content_hash_value:
            return None
        return base64.b64encode(self.content_hash_value).decode()


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

    def to_dict(self, detail=False):
        ret = dict(
            status=self.status,
            url=self.url,
            title=self.title,
            link=self.link,
            author=self.author,
            icon=self.icon,
            description=self.description,
            version=self.version,
            dt_updated=self.dt_updated,
            dt_created=self.dt_created,
            dt_checked=self.dt_checked,
            dt_synced=self.dt_synced,
        )
        if detail:
            ret.update(
                encoding=self.encoding,
                etag=self.etag,
                last_modified=self.last_modified,
                content_length=self.content_length,
                content_hash_method=self.content_hash_method,
                content_hash_value=self.content_hash_value_base64,
            )
        return ret


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
    content = models.BinaryField(**optional)
    content_length = models.IntegerField(
        **optional, help_text='length of content')
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")


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
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")

    def to_dict(self, detail=False):
        if self.feed_id:
            ret = self.feed.to_dict(detail=detail)
        else:
            ret = dict(url=self.url, dt_updated=self.dt_created)
        ret.update(
            id=self.id,
            user=dict(id=self.user_id),
            dt_created=self.dt_created,
        )
        if self.title:
            ret.update(title=self.title)
        if (not self.status) or (self.status != FeedStatus.READY):
            ret.update(status=self.status)
        return ret


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
