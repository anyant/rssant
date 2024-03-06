import logging
import re

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
    try:
        result = _get_publish_info(request)
    except PermissionDenied:
        return dict(is_enable=False)
    else:
        return result.to_dict()


validate_publish_unionid = schema_compiler.compile(T.publish_unionid.object)

RE_PUBLISH_API = re.compile(r'^/api/[^/]+/publish\.', re.I)


def _is_publish_api(request):
    return bool(RE_PUBLISH_API.match(request.path))


def _get_publish_header(request):
    value = request.headers.get('x-rssant-publish')
    return value and value.strip()


def _get_publish_info_by_header(header):
    try:
        publish_unionid = validate_publish_unionid(header)
    except Invalid as ex:
        raise ValidationError('invalid x-rssant-publish') from ex
    publish_info = UserPublish.get_cached(
        publish_id=publish_unionid.publish_id,
        user_id=publish_unionid.user_id,
    )
    return publish_info


def _get_publish_info_by_user(user):
    publish_info = UserPublish.get_cached(user_id=user.id)
    return publish_info


def _get_publish_info_impl(request):
    value: str = _get_publish_header(request)
    if value:
        return _get_publish_info_by_header(value)
    # 取登录用户信息，方便直接预览自己发布的订阅
    if request.user.is_authenticated:
        return _get_publish_info_by_user(request.user)
    return None


def _get_publish_info(request):
    publish_info = _get_publish_info_impl(request)
    if publish_info is None or (not publish_info.is_enable):
        raise PermissionDenied('rssant publish not available')
    return publish_info


def require_publish_user(request):
    if not _is_publish_api(request):
        return request.user
    publish_info = _get_publish_info(request)
    user = UserPublish.get_user_cached(publish_info.user_id)
    if user is None:
        raise RuntimeError('rssant publish user not found')
    return user


def is_only_publish(request):
    if not _is_publish_api(request):
        return False
    publish_info = _get_publish_info(request)
    return not publish_info.is_all_public
