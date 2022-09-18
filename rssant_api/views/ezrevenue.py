from django.contrib.auth.models import AbstractUser
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_common.ezrevenue import EZREVENUE_CLIENT

EzrevenueView = RestRouter()


@EzrevenueView.post('ezrevenue/customer.info')
def ezrevenue_customer_info(
    request,
    include_balance: T.bool.default(True),
) -> T.dict:
    if not EZREVENUE_CLIENT:
        return Response(status=501)
    user: AbstractUser = request.user
    params = dict(
        paywall_alias='paywall_vip',
        customer=dict(
            external_id=user.id,
            nickname=user.username,
            external_dt_created=user.date_joined.isoformat(),
        ),
        include_balance=include_balance,
    )
    return EZREVENUE_CLIENT.call('customer.info', params)
