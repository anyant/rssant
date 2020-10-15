from shopant_sdk import ShopAntServer
from rssant_config import CONFIG


SHOPANT_SERVER = None
if CONFIG.shopant_enable:
    SHOPANT_SERVER = ShopAntServer(
        product_id=CONFIG.shopant_product_id,
        product_secret=CONFIG.shopant_product_secret,
        url=CONFIG.shopant_url,
    )
