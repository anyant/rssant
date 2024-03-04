from functools import cached_property
from threading import RLock
from typing import Optional

from cachetools import TTLCache, cached

from rssant_common.validator import PublishUnionId

from .helper import Model, User, models, optional


class UserPublish(Model):
    """用户订阅发布页面配置"""

    class Meta:
        indexes = [
            models.Index(fields=['user']),
            models.Index(fields=['root_url']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['user'], name='unique_user'),
        ]

    class Admin:
        display_fields = ['user', 'is_enable', 'root_url']

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    is_enable = models.BooleanField(
        **optional, default=False, verbose_name='是否启用'
    )
    root_url: str = models.CharField(
        **optional, max_length=255, verbose_name='访问地址'
    )
    is_all_public = models.BooleanField(
        **optional, default=False, verbose_name='是否全部公开'
    )
    dt_created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    dt_updated = models.DateTimeField(**optional, help_text="更新时间")

    @cached_property
    def unionid(self):
        return PublishUnionId(self.user_id, self.id)

    def to_dict(self):
        return dict(
            unionid=self.unionid,
            is_enable=self.is_enable,
            root_url=self.root_url,
            is_all_public=self.is_all_public,
            dt_created=self.dt_created,
            dt_updated=self.dt_updated,
        )

    @classmethod
    def _get_impl(
        cls,
        *,
        publish_id: int = None,
        user_id: int = None,
    ) -> Optional["UserPublish"]:
        q = UserPublish.objects
        if publish_id is not None:
            q = q.filter(id=publish_id)
        if user_id is not None:
            q = q.filter(user_id=user_id)
        result = q.seal().first()
        return result

    @classmethod
    @cached(
        TTLCache(maxsize=1000, ttl=30),
        lock=RLock(),
    )
    def _get_impl_cached(cls, *args, **kwargs):
        return cls._get_impl(*args, **kwargs)

    @classmethod
    def get(
        cls,
        *,
        publish_id: int = None,
        user_id: int = None,
    ) -> Optional["UserPublish"]:
        if publish_id is None and user_id is None:
            raise ValueError('publish_id or user_id required')
        return cls._get_impl(publish_id=publish_id, user_id=user_id)

    @classmethod
    def get_cached(
        cls,
        *,
        publish_id: int = None,
        user_id: int = None,
    ) -> Optional["UserPublish"]:
        if publish_id is None and user_id is None:
            raise ValueError('publish_id or user_id required')
        return cls._get_impl_cached(publish_id=publish_id, user_id=user_id)

    @classmethod
    def set(
        cls,
        *,
        user_id: int,
        publish_id: int = None,
        is_enable: bool,
        root_url: str = None,
        is_all_public: bool = None,
    ) -> "UserPublish":
        data = dict(
            is_enable=is_enable,
            root_url=root_url,
            is_all_public=is_all_public,
        )
        if publish_id is None:
            return UserPublish.objects.create(user_id=user_id, **data)
        q = UserPublish.objects.filter(id=publish_id, user_id=user_id)
        updated = q.update(**data)
        if updated <= 0:
            raise ValueError(f'update UserPublish#{publish_id} failed')
        return cls.get(publish_id=publish_id, user_id=user_id)

    @classmethod
    def _get_user_impl(cls, pk):
        return User.objects.filter(pk=pk).first()

    @classmethod
    @cached(
        TTLCache(maxsize=1000, ttl=30),
        lock=RLock(),
    )
    def get_user_cached(cls, *args, **kwargs):
        return cls._get_user_impl(*args, **kwargs)

    @classmethod
    def internal_clear_cache(cls):
        with cls._get_impl_cached.cache_lock:
            cls._get_impl_cached.cache.clear()
        with cls.get_user_cached.cache_lock:
            cls.get_user_cached.cache.clear()
