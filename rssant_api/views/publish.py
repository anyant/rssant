import logging

from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.permissions import AllowAny
from validr import Invalid

from django_rest_validr import RestRouter, T
from rssant_api.models.user_publish import UserPublish
from rssant_common.validator import compiler as schema_compiler

from .user_publish import UserPublishSchema

LOG = logging.getLogger(__name__)


PublishView = RestRouter(permission_classes=[AllowAny])


@PublishView.post('publish.info')
def on_publish_info(request) -> UserPublishSchema:
    """公开获取发布页面配置"""
    value: str = _get_publish_header(request)
    if not value:
        raise ValidationError('missing x-rssant-publish')
    result = _get_publish_info(value)
    if result is None:
        return dict(is_enable=False)
    return result.to_dict()


validate_publish_unionid = schema_compiler.compile(T.publish_unionid.object)


def _get_publish_header(request):
    value = request.headers.get('x-rssant-publish')
    return value and value.strip()


def _get_publish_info(header):
    try:
        publish_unionid = validate_publish_unionid(header)
    except Invalid as ex:
        raise ValidationError('invalid x-rssant-publish') from ex
    publish_info = UserPublish.get_cached(
        publish_id=publish_unionid.publish_id,
        user_id=publish_unionid.user_id,
    )
    return publish_info


def _get_publish_user(request):
    value: str = _get_publish_header(request)
    if not value:
        return None
    publish_info = _get_publish_info(value)
    if publish_info is None or (not publish_info.is_enable):
        raise PermissionDenied('rssant publish not available')
    user = UserPublish.get_user_cached(publish_info.user_id)
    if user is None:
        raise RuntimeError('rssant publish user not found')
    return user


def require_publish_user(request):
    user = _get_publish_user(request)
    if user is None:
        if request.user.is_authenticated:
            user = request.user
    if user is None:
        raise PermissionDenied()
    return user


def is_publish_request(request):
    return bool(_get_publish_header(request))
