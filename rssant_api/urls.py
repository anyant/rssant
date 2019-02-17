from django.urls import path, include

from .views.feed import FeedView

app_name = 'rssant_api'
urlpatterns = [
    path('', include(FeedView.urls)),
]
