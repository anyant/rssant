from django_rest_validr import RestRouter, T
from django.contrib import auth as django_auth
from django.contrib.auth.models import User
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.permissions import AllowAny
from allauth.socialaccount.models import SocialAccount


UserSchema = T.dict(
    id=T.int,
    username=T.str,
    avatar_url=T.str.optional,
    token=T.str.optional,
)

UserView = RestRouter(permission_classes=[AllowAny])


def serialize_user(user):
    try:
        social_account = SocialAccount.objects.get(user=user)
        avatar_url = social_account.get_avatar_url()
    except SocialAccount.DoesNotExist:
        avatar_url = None
    token, created = Token.objects.get_or_create(user=user)
    return dict(
        id=user.id,
        username=user.username,
        avatar_url=avatar_url,
        token=token.key,
    )


@UserView.post('user/login/')
def user_login(
    request,
    account: T.str.optional,
    password: T.str.optional,
) -> UserSchema:
    if not (account or password):
        if request.user.is_authenticated:
            if not request.user.is_active:
                return Response(status=403)
            return serialize_user(request.user)
        return Response(status=401)
    if '@' in account:
        tmp_user = User.objects.get(email=account)
        if tmp_user:
            username = tmp_user.username
        else:
            return Response({'message': 'email not exists'}, status=401)
    else:
        username = account
    user = django_auth.authenticate(username=username, password=password)
    if not user or not user.is_authenticated:
        return Response(status=401)
    if not user.is_active:
        return Response(status=403)
    django_auth.login(request, user=user)
    return serialize_user(request.user)
