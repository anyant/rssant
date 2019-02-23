from django.contrib import admin

from . import models


def _register(model):
    if not hasattr(model, 'Admin'):
        return admin.site.register(m)

    class RssantModelAdmin(admin.ModelAdmin):
        if hasattr(model.Admin, 'display_fields'):
            list_display = tuple(['id'] + model.Admin.display_fields)
        if hasattr(model.Admin, 'search_fields'):
            search_fields = model.Admin.search_fields

    return admin.site.register(m, RssantModelAdmin)


for m in models.__models__:
    _register(m)
