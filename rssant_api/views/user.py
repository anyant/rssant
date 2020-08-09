from django_rest_validr import RestRouter, T
from django.contrib import auth as django_auth
from django.contrib.auth.models import User
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.permissions import AllowAny
from rest_framework.exceptions import AuthenticationFailed
from allauth.socialaccount.models import SocialAccount


UserSchema = T.dict(
    id=T.int,
    username=T.str,
    has_usable_password=T.bool.optional,
    avatar_url=T.str.optional,
    token=T.str.optional,
    social_accounts=T.list(T.dict(
        provider=T.str,
        avatar_url=T.str.optional,
    )).optional
)

UserView = RestRouter(permission_classes=[AllowAny])


def serialize_user(user):
    avatar_url = None
    social_accounts_info = []
    social_accounts = list(SocialAccount.objects.filter(user=user).all())
    for acc in social_accounts:
        if not avatar_url:
            avatar_url = acc.get_avatar_url()
        social_accounts_info.append(dict(
            provider=acc.provider,
            avatar_url=acc.get_avatar_url(),
        ))
    token, created = Token.objects.get_or_create(user=user)
    has_usable_password = user.password and user.has_usable_password()
    return dict(
        id=user.id,
        username=user.username,
        has_usable_password=has_usable_password,
        avatar_url=avatar_url,
        token=token.key,
        social_accounts=social_accounts_info,
    )


@UserView.post('user/login/')
def user_login(
    request,
    account: T.str.optional,
    password: T.str.optional,
) -> UserSchema:
    deactive_message = {'message': '账户状态异常，请联系作者'}
    error_message = {'message': '账号或密码错误'}
    if not (account or password):
        if request.user.is_authenticated:
            if not request.user.is_active:
                return Response(deactive_message, status=403)
            return serialize_user(request.user)
        return Response(error_message, status=401)
    if '@' in account:
        tmp_user = User.objects.filter(email=account).first()
        if tmp_user:
            username = tmp_user.username
        else:
            return Response(error_message, status=401)
    else:
        username = account
    try:
        user = django_auth.authenticate(username=username, password=password)
    except AuthenticationFailed:
        return Response(error_message, status=401)
    if not user or not user.is_authenticated:
        return Response(error_message, status=401)
    if not user.is_active:
        return Response(deactive_message, status=403)
    django_auth.login(request, user=user)
    return serialize_user(request.user)
