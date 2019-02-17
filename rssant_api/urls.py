from django.urls import path, include

from .views.feed import FeedView
from .views.task import TaskView

app_name = 'rssant_api'
urlpatterns = [
    path('', include(FeedView.urls)),
    path('', include(TaskView.urls)),
]
