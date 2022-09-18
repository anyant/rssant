from django.urls import include, path

from .views import error, index
from .views.ezrevenue import EzrevenueView
from .views.feed import FeedView
from .views.story import StoryView
from .views.user import UserView

app_name = 'rssant_api'
urlpatterns = [
    path('', index),
    path('error/', error),
    path('', include(FeedView.urls)),
    path('', include(StoryView.urls)),
    path('', include(UserView.urls)),
    path('', include(EzrevenueView.urls)),
]
