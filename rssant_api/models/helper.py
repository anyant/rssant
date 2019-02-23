from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField

optional = dict(blank=True, null=True)


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


__all__ = (
    'models',
    'Model',
    'optional',
    'User',
    'JSONField',
)
