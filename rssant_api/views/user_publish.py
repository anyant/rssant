import logging

from django_rest_validr import RestRouter, T
from rssant_api.models.user_publish import UserPublish

from .helper import check_unionid

LOG = logging.getLogger(__name__)

UserPublishSchema = T.dict(
    unionid=T.publish_unionid.optional,
    is_enable=T.bool.optional,
    root_url=T.nstr.optional,
    website_title=T.nstr.optional,
    is_all_public=T.bool.optional,
    dt_created=T.datetime.object.optional,
    dt_updated=T.datetime.object.optional,
    image_proxy=T.dict(
        enable=T.bool,
        url_s=T.list(T.str).optional,
    ).optional,
)

UserPublishView = RestRouter()


@UserPublishView.post('user_publish.get')
def on_user_publish_get(request) -> UserPublishSchema:
    """用户获取发布页面配置"""
    user_id = request.user.id
    result = UserPublish.get(user_id=user_id)
    if result is None:
        return dict(is_enable=False)
    return result.to_dict()


_RootUrlSchema = T.url.scheme('http https').maxlen(255).optional


@UserPublishView.post('user_publish.set')
def on_user_publish_set(
    request,
    unionid: T.publish_unionid.object.optional,
    is_enable: T.bool,
    root_url: _RootUrlSchema,
    website_title: T.nstr.optional,
    is_all_public: T.bool.optional,
) -> UserPublishSchema:
    """用户设置发布页面配置"""
    publish_id = None
    if unionid:
        check_unionid(request.user, unionid)
        publish_id = unionid.publish_id
    user_id = request.user.id
    result = UserPublish.set(
        user_id=user_id,
        publish_id=publish_id,
        is_enable=is_enable,
        root_url=root_url,
        website_title=website_title,
        is_all_public=is_all_public,
    )
    return result.to_dict()
