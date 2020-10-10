from django_rest_validr import RestRouter, T
from rest_framework.response import Response

from shopant_sdk import ShopAntServer
from rssant_config import CONFIG


ShopantView = RestRouter()


SHOPANT_SERVER = None
if CONFIG.shopant_enable:
    SHOPANT_SERVER = ShopAntServer(
        product_id=CONFIG.shopant_product_id,
        product_secret=CONFIG.shopant_product_secret,
        url=CONFIG.shopant_url,
    )


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
