from django.urls import path, include

from .views import index
from .views.feed import FeedView
from .views.story import StoryView
from .views.task import TaskView
from .views.user import UserView

app_name = 'rssant_api'
urlpatterns = [
    path('v1/index', index),
    path('v1/', include(FeedView.urls)),
    path('v1/', include(StoryView.urls)),
    path('v1/', include(TaskView.urls)),
    path('v1/', include(UserView.urls)),
]
