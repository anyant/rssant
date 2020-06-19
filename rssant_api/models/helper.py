from django.db import models
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields import JSONField
# https://github.com/gavinwahl/django-optimistic-lock
from ool import VersionField, VersionedMixin, ConcurrentUpdate as ConcurrentUpdateError
# https://github.com/charettes/django-seal
from seal.models import SealableModel

from rssant.helper.content_hash import compute_hash_base64

User = get_user_model()
optional = dict(blank=True, null=True)


def extract_choices(cls):
    return [(v, v)for k, v in vars(cls).items() if k.isupper()]


class Model(VersionedMixin, SealableModel):

    class Meta:
        abstract = True

    _version = VersionField()
    _created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    _updated = models.DateTimeField(auto_now=True, help_text="更新时间")

    def __str__(self):
        default = f'{self.__class__.__name__}#{self.id}'
        admin = getattr(self.__class__, 'Admin', None)
        if not admin:
            return default
        fields = getattr(admin, 'display_fields')
        if not fields:
            return default
        details = []
        for field in fields:
            value = getattr(self, field)
            details.append(f'{field}={value}')
        details = ' '.join(details)
        return f'{self.__class__.__name__}#{self.id} {details}'


class ContentHashMixin(SealableModel):

    class Meta:
        abstract = True

    content_hash_base64 = models.CharField(
        max_length=200, **optional, help_text='base64 hash value of content')

    def is_modified(self, content_hash_base64=None, fields=None):
        if content_hash_base64 is None and fields:
            content_hash_base64 = compute_hash_base64(*fields)
        if content_hash_base64 is not None:
            return content_hash_base64 != self.content_hash_base64
        return True


__all__ = (
    'models',
    'Model',
    'User',
    'JSONField',
    'ContentHashMixin',
    'ConcurrentUpdateError',
)
