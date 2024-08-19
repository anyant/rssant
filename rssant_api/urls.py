from django.urls import include, path

from rssant_harbor.view import HarborView
from rssant_worker.view import WorkerView

from .views import error, index
from .views.ezrevenue import EzrevenueView
from .views.feed import FeedView
from .views.publish import PublishView
from .views.story import StoryView
from .views.user import UserView
from .views.user_publish import UserPublishView

app_name = 'rssant_api'
urlpatterns = [
    path('', index),
    path('error/', error),
    path('', include(HarborView.urls)),
    path('', include(WorkerView.urls)),
    path('', include(FeedView.urls)),
    path('', include(StoryView.urls)),
    path('', include(UserView.urls)),
    path('', include(PublishView.urls)),
    path('', include(UserPublishView.urls)),
    path('', include(EzrevenueView.urls)),
]
