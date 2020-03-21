from django.http import HttpResponse

from rssant_config import CONFIG
from rssant_common.changelog import ChangeLogList


def index(request):
    return HttpResponse("你好, RSSAnt!")


def accounts_profile(request):
    user = request.user
    if user.is_authenticated:
        msg = f"Hi, {user.username}!"
    else:
        msg = f'Hi, welcome!'
    return HttpResponse(msg)


CHANGE_LOGS = ChangeLogList(
    directory='docs/changelog',
    title='蚁阅更新日志',
    link=CONFIG.root_url,
)

CHANGE_LOGS_ATOM = CHANGE_LOGS.to_atom()
CHANGE_LOGS_HTML = CHANGE_LOGS.to_html()


def changelog_atom(request):
    return HttpResponse(
        CHANGE_LOGS_ATOM, content_type='text/xml', charset='utf-8')


def changelog_html(request):
    return HttpResponse(
        CHANGE_LOGS_HTML, content_type='text/html', charset='utf-8')
