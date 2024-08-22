from django.urls import include, path

from rssant_config import CONFIG
from rssant_harbor.view import HarborView
from rssant_worker.view import WorkerView

from .views import health
from .views.ezrevenue import EzrevenueView
from .views.feed import FeedView
from .views.publish import PublishView
from .views.story import StoryView
from .views.user import UserView
from .views.user_publish import UserPublishView


def _gen_urlpatterns():
    yield path('', include(health.urls))
    if CONFIG.is_role_worker:
        yield path('', include(WorkerView.urls))
    else:
        yield path('', include(FeedView.urls))
        yield path('', include(StoryView.urls))
        yield path('', include(UserView.urls))
        yield path('', include(PublishView.urls))
        yield path('', include(UserPublishView.urls))
        yield path('', include(EzrevenueView.urls))
        yield path('', include(HarborView.urls))


app_name = 'rssant_api'
urlpatterns = list(_gen_urlpatterns())
