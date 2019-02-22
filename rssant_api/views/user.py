from django_rest_validr import RestRouter, T
from allauth.socialaccount.models import SocialAccount

UserSchema = T.dict(
    id=T.int,
    username=T.str,
    avatar_url=T.str.optional,
)

UserView = RestRouter()


@UserView.get('user/me')
def user_me(request) -> UserSchema:
    social_account = SocialAccount.objects.get(user=request.user)
    avatar_url = social_account.get_avatar_url()
    return dict(
        id=request.user.id,
        username=request.user.username,
        avatar_url=avatar_url,
    )
