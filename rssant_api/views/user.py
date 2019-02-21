from django.contrib.auth.decorators import login_required
from django_rest_validr import RestRouter, T


UserSchema = T.dict(
    id=T.int,
    username=T.str,
)

UserView = RestRouter()


@UserView.get('user/me')
@login_required
def user_me(request) -> UserSchema:
    return request.user
