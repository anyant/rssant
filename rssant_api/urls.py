from django.urls import path, include

from . import views

app_name = 'rssant_api'
urlpatterns = [
    path('', include(views.FeedView.urls)),
]
