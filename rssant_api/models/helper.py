from django.db import models
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields import JSONField

from rssant.helper.content_hash import compute_hash_base64

User = get_user_model()
optional = dict(blank=True, null=True)


def extract_choices(cls):
    return [(v, v)for k, v in vars(cls).items() if k.isupper()]


class Model(models.Model):

    class Meta:
        abstract = True

    _created = models.DateTimeField(auto_now_add=True, help_text="创建时间")
    _updated = models.DateTimeField(auto_now=True, help_text="更新时间")

    def __str__(self):
        default = f'{self.__class__.__name__}#{self.id}'
        admin = getattr(self.__class__, 'Admin')
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


class ContentHashMixin(models.Model):

    class Meta:
        abstract = True

    content_hash_base64 = models.CharField(
        max_length=200, **optional, help_text='base64 hash value of content')

    def is_modified(self, *fields, content_hash_base64=None):
        if content_hash_base64:
            return content_hash_base64 != self.content_hash_base64
        if not self.content_hash_base64:
            return True
        content_hash_base64 = compute_hash_base64(*fields)
        return content_hash_base64 != self.content_hash_base64


__all__ = (
    'models',
    'Model',
    'User',
    'JSONField',
    'ContentHashMixin',
)
