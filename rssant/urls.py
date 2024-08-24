"""rssant URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from rest_framework.documentation import include_docs_urls
from rest_framework.schemas import get_schema_view
from rest_framework_swagger.views import get_swagger_view

from rssant_config import CONFIG

from . import auth, views
from .allauth_providers.github import urls as github_urls

API_TITLE = 'RSSAnt API'
API_DESCRIPTION = 'A Web API for RSSAnt.'


def _gen_urlpatterns():
    if settings.DEBUG:
        import debug_toolbar

        yield path('__debug__/', include(debug_toolbar.urls))
    yield path('', views.index)
    if not CONFIG.is_role_worker:
        schema_view = get_schema_view(title=API_TITLE, description=API_DESCRIPTION)
        docs_view = include_docs_urls(title=API_TITLE, description=API_DESCRIPTION)
        swagger_view = get_swagger_view(title=API_TITLE)
        yield path('changelog', views.changelog_html)
        yield path('changelog.atom', views.changelog_atom)
        yield path('admin/', admin.site.urls)
        yield path('docs/v1/', docs_view)
        yield path(
            'docs/v1/', include('rest_framework.urls', namespace='rest_framework')
        )
        yield path('docs/v1/schema/', schema_view)
        yield path('docs/v1/swagger/', swagger_view)
        yield path('api/v1/accounts/profile/', views.accounts_profile)
        yield path('api/v1/analytics.js', views.analytics_script)
        # override allauth github views
        yield path('api/v1/accounts/', include(github_urls))
        yield path('api/v1/accounts/', include('allauth.urls'))
        yield path('api/v1/', include(auth.urlpaterns))
    yield path('api/v1/', include('rssant_api.urls'))


urlpatterns = list(_gen_urlpatterns())
