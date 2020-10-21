from django.urls import path, include

from .views import index, error
from .views.feed import FeedView
from .views.story import StoryView
from .views.user import UserView
from .views.shopant import ShopantView
from .views.image import ImageView

app_name = 'rssant_api'
urlpatterns = [
    path('', index),
    path('error/', error),
    path('', include(FeedView.urls)),
    path('', include(StoryView.urls)),
    path('', include(UserView.urls)),
    path('', include(ShopantView.urls)),
    path('', include(ImageView.urls)),
]
