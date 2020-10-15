from django_rest_validr import RestRouter, T
from rest_framework.response import Response
from rssant_common.shopant import SHOPANT_SERVER


ShopantView = RestRouter()


@ShopantView.post('shopant/integration')
def shopant_integration(
    request,
    method: T.str,
    params: T.dict.key(T.str).optional,
) -> T.dict:
    if not SHOPANT_SERVER:
        return Response(status=501)
    if not params:
        params = {}
    user = request.user
    params['customer'] = dict(
        external_id=user.id,
        nickname=user.username,
    )
    return SHOPANT_SERVER.integration(dict(method=method, params=params))
