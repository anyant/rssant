from django.http import HttpResponse


def index(request):
    return HttpResponse("你好, RSSAnt!")


def accounts_profile(request):
    user = request.user
    if user.is_authenticated:
        msg = f"Hi, {user.username}!"
    else:
        msg = f'Hi, welcome!'
    return HttpResponse(msg)
