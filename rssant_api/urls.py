from django.urls import path, include

from .views import index, error
from .views.feed import FeedView
from .views.story import StoryView
from .views.task import TaskView
from .views.user import UserView
from .views.async_callback import AsyncCallbackView

app_name = 'rssant_api'
urlpatterns = [
    path('', index),
    path('error/', error),
    path('', include(FeedView.urls)),
    path('', include(StoryView.urls)),
    path('', include(TaskView.urls)),
    path('', include(UserView.urls)),
    path('', include(AsyncCallbackView.urls)),
]
