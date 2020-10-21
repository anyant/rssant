from django_rest_validr import RestRouter, T
from rssant_api.models import ImageInfo
from rssant_common.image_token import ImageToken
from rssant_config import CONFIG
from rssant_feedlib.response import FeedResponseStatus


ImageView = RestRouter()


@ImageView.post('image-proxy/check')
def image_check(
    request,
    referrer: T.url.optional,
    images: T.list(T.dict(
        url_root=T.url,
    ))
) -> T.dict(
    images=T.list(T.dict(
        url_root=T.url,
        proxy_mode=T.enum('proxy fallback direct'),
        referrer_policy=T.enum('no-referrer origin'),
        token=T.str,
    ))
):
    # TODO: verify url_root
    url_roots = {x['url_root'] for x in images}
    url_root_map = ImageInfo.batch_detect(list(url_roots))
    result = []
    for url_root in url_roots:
        status = url_root_map.get(url_root)
        proxy_mode = 'fallback'
        if status is not None:
            if FeedResponseStatus.is_referrer_deny(status):
                proxy_mode = 'proxy'
        token = ImageToken(url_root, referrer=referrer)\
            .encode(secret=CONFIG.image_token_secret)
        result.append(dict(
            url_root=url_root,
            proxy_mode=proxy_mode,
            token=token,
            referrer_policy='no-referrer',
        ))
    return dict(images=result)
