import logging
from datetime import timedelta
from threading import RLock

from cachetools import TTLCache
from django.utils import timezone

from .feed import Feed, FeedStatus
from .helper import Model, models, optional

LOG = logging.getLogger(__name__)

DISCOVER_CACHE_TTL = 10 * 60
DISCOVER_FEED_MAX_AGE_DAYS = 30
DISCOVER_LIST_PAGE_SIZE = 100
DISCOVER_EDITOR_INTRO_MAX_LENGTH = 500


class DiscoverError(Exception):
    """发现页管理操作失败"""


class DiscoverCategory(Model):
    """发现页分类"""

    class Meta:
        indexes = [
            models.Index(fields=['deleted_at', 'rank']),
        ]

    class Admin:
        display_fields = ['slug', 'name', 'rank', 'deleted_at']

    slug = models.CharField(max_length=64, unique=True, help_text='接口参数')
    name = models.CharField(max_length=128, help_text='展示名')
    rank = models.IntegerField(default=0, help_text='排序，小的在前')
    deleted_at = models.DateTimeField(**optional, help_text='软删除时间')

    def to_dict(self):
        return dict(
            slug=self.slug,
            name=self.name,
            rank=self.rank,
            deleted_at=self.deleted_at,
        )


class DiscoverFeed(Model):
    """发现页分类里的源"""

    class Meta:
        indexes = [
            models.Index(fields=['category', 'deleted_at', 'rank']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['feed', 'category'], name='unique_discover_feed'
            ),
        ]

    class Admin:
        display_fields = ['feed_id', 'category_id', 'rank', 'deleted_at']

    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    category = models.ForeignKey(DiscoverCategory, on_delete=models.CASCADE)
    editor_intro = models.TextField(**optional, help_text='编辑写的简介')
    rank = models.IntegerField(default=0, help_text='分类内排序')
    deleted_at = models.DateTimeField(**optional, help_text='软删除时间')

    def to_dict(self):
        feed = self.feed
        return dict(
            category=self.category.slug,
            url=feed.url,
            title=feed.title,
            status=feed.status,
            dt_checked=feed.dt_checked,
            dt_latest_story_published=feed.dt_latest_story_published,
            freeze_level=feed.freeze_level,
            dryness=feed.dryness,
            editor_intro=self.editor_intro,
            rank=self.rank,
            deleted_at=self.deleted_at,
        )


class DiscoverService:
    """发现页数据读写和公开数据缓存"""

    _cache = TTLCache(maxsize=1, ttl=DISCOVER_CACHE_TTL)
    _cache_lock = RLock()

    @classmethod
    def get_public_data(cls):
        with cls._cache_lock:
            data = cls._cache.get('data')
            if data is None:
                data = cls._build_public_data()
                cls._cache['data'] = data
                LOG.info('discover public data cache rebuilt')
            return data

    @classmethod
    def clear_cache(cls):
        with cls._cache_lock:
            cls._cache.clear()

    @classmethod
    def _build_public_data(cls):
        cutoff = timezone.now() - timedelta(days=DISCOVER_FEED_MAX_AGE_DAYS)
        categories = list(
            DiscoverCategory.objects.filter(deleted_at__isnull=True)
            .order_by('rank', 'id')
        )
        items = (
            DiscoverFeed.objects.select_related('feed', 'category')
            .filter(
                deleted_at__isnull=True,
                category__deleted_at__isnull=True,
                feed__status=FeedStatus.READY,
                feed__dt_checked__gte=cutoff,
            )
            .order_by('category__rank', 'category_id', 'rank', 'id')
        )
        return dict(
            categories=[
                dict(slug=category.slug, name=category.name)
                for category in categories
            ],
            items=[
                dict(
                    category=item.category.slug,
                    url=item.feed.url,
                    title=item.feed.title,
                    link=item.feed.link,
                    editor_intro=item.editor_intro,
                    dt_latest_story_published=item.feed.dt_latest_story_published,
                    freeze_level=item.feed.freeze_level,
                    dryness=item.feed.dryness,
                )
                for item in items
            ],
        )

    @classmethod
    def get_categories(cls):
        return list(DiscoverCategory.objects.order_by('rank', 'id'))

    @classmethod
    def upsert_category(cls, *, slug, name=None, rank=None):
        if not slug:
            raise DiscoverError('slug 不能为空')
        if name is not None and not name:
            raise DiscoverError('分类名称不能为空')
        category = DiscoverCategory.objects.filter(slug=slug).first()
        if category is None:
            if not name:
                raise DiscoverError('新建分类需要名称')
            category = DiscoverCategory(slug=slug, name=name, rank=rank or 0)
            category.save()
            cls.clear_cache()
            return category
        changed = False
        if name is not None:
            category.name = name
            changed = True
        if rank is not None:
            category.rank = rank
            changed = True
        if category.deleted_at is not None:
            category.deleted_at = None
            changed = True
        if changed:
            category.save()
            cls.clear_cache()
        return category

    @classmethod
    def delete_category(cls, *, slug):
        category = DiscoverCategory.objects.filter(slug=slug).first()
        if category is None:
            raise DiscoverError('分类不存在')
        if category.deleted_at is None:
            category.deleted_at = timezone.now()
            category.save()
            cls.clear_cache()
        return category

    @classmethod
    def list_items(cls, *, category_slug=None, include_deleted=False, page=1):
        query = DiscoverFeed.objects.select_related('feed', 'category')
        if category_slug:
            category = DiscoverCategory.objects.filter(slug=category_slug).first()
            if category is None:
                raise DiscoverError('分类不存在')
            query = query.filter(category=category)
        if not include_deleted:
            query = query.filter(
                deleted_at__isnull=True,
                category__deleted_at__isnull=True,
            )
        total = query.count()
        offset = (page - 1) * DISCOVER_LIST_PAGE_SIZE
        items = query.order_by('category__rank', 'category_id', 'rank', 'id')[
            offset : offset + DISCOVER_LIST_PAGE_SIZE
        ]
        return total, list(items)

    @classmethod
    def upsert_item(cls, *, url, category_slug, editor_intro=None, rank=None):
        feed = Feed.objects.filter(url=url).first()
        if feed is None:
            raise DiscoverError('源地址不存在，请先用订阅接口解析这个地址')
        category = DiscoverCategory.objects.filter(slug=category_slug).first()
        if category is None or category.deleted_at is not None:
            raise DiscoverError('分类不存在或已删除')
        if feed.status == FeedStatus.DISCARD:
            feed.revive()
        item = DiscoverFeed.objects.filter(feed=feed, category=category).first()
        if item is None:
            item = DiscoverFeed(
                feed=feed,
                category=category,
                editor_intro=editor_intro,
                rank=rank if rank is not None else 0,
            )
            item.save()
            cls.clear_cache()
            return item
        changed = False
        if editor_intro is not None:
            item.editor_intro = editor_intro
            changed = True
        if rank is not None:
            item.rank = rank
            changed = True
        if item.deleted_at is not None:
            item.deleted_at = None
            changed = True
        if changed:
            item.save()
            cls.clear_cache()
        return item

    @classmethod
    def remove_item(cls, *, url, category_slug):
        feed = Feed.objects.filter(url=url).first()
        if feed is None:
            raise DiscoverError('源地址不存在')
        category = DiscoverCategory.objects.filter(slug=category_slug).first()
        if category is None:
            raise DiscoverError('分类不存在')
        item = DiscoverFeed.objects.filter(feed=feed, category=category).first()
        if item is None:
            raise DiscoverError('这个源不在该分类里')
        if item.deleted_at is None:
            item.deleted_at = timezone.now()
            item.save()
            cls.clear_cache()
        return item
