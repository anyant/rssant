from django.urls import path, include

from .views import index
from .views.feed import FeedView
from .views.story import StoryView
from .views.task import TaskView

app_name = 'rssant_api'
urlpatterns = [
    path('', index),
    path('', include(FeedView.urls)),
    path('', include(StoryView.urls)),
    path('', include(TaskView.urls)),
]
