from django.contrib import admin

from . import models

__models__ = [getattr(models, x) for x in models.__all__]
for m in __models__:
    admin.site.register(m)
