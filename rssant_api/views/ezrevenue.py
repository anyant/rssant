from django.contrib.auth.models import AbstractUser
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_api.models.user_profile import UserProfile
from rssant_common.ezrevenue import EZREVENUE_CLIENT

EzrevenueView = RestRouter()


@EzrevenueView.post('ezrevenue/customer.info')
def ezrevenue_customer_info(
    request,
) -> T.dict:
    if not EZREVENUE_CLIENT:
        return Response(status=501)
    user: AbstractUser = request.user
    profile = UserProfile.sync_vip_info(user=user)
    return profile.vip_info
