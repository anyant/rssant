from django.http import HttpResponse
from django.utils import timezone
from django.views.decorators.cache import cache_control
from django.views.decorators.http import condition

from rssant_common.analytics import AnalyticsScript
from rssant_common.changelog import ChangeLogList
from rssant_common.standby_domain import get_request_domain
from rssant_config import CONFIG


def index(request):
    msg = "你好, RSSAnt {}!".format(CONFIG.role)
    return HttpResponse(msg)


def accounts_profile(request):
    user = request.user
    if user.is_authenticated:
        msg = f"Hi, {user.username}!"
    else:
        msg = 'Hi, welcome!'
    return HttpResponse(msg)


CHANGE_LOGS_LAST_MODIFIED = timezone.now()
CHANGE_LOGS_MAP = {}


def _init_changelog():
    for domain in {CONFIG.root_domain} | CONFIG.standby_domain_set:
        link = CONFIG.root_url.replace(CONFIG.root_domain, domain)
        changelog = ChangeLogList(
            directory='docs/changelog',
            title='蚁阅更新日志',
            link=link,
        )
        atom = changelog.to_atom()
        html = changelog.to_html()
        CHANGE_LOGS_MAP[domain] = dict(atom=atom, html=html)


_init_changelog()


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: CHANGE_LOGS_LAST_MODIFIED)
def changelog_atom(request):
    atom = CHANGE_LOGS_MAP[get_request_domain(request)]['atom']
    return HttpResponse(atom, content_type='text/xml', charset='utf-8')


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: CHANGE_LOGS_LAST_MODIFIED)
def changelog_html(request):
    html = CHANGE_LOGS_MAP[get_request_domain(request)]['html']
    return HttpResponse(html, content_type='text/html', charset='utf-8')


ANALYTICS_SCRIPT_LAST_MODIFIED = timezone.now()
ANALYTICS_SCRIPT_MAP = {}


def _init_analytics():
    for domain in {CONFIG.root_domain} | CONFIG.standby_domain_set:
        script = AnalyticsScript().generate(request_domain=domain)
        ANALYTICS_SCRIPT_MAP[domain] = script


_init_analytics()


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: ANALYTICS_SCRIPT_LAST_MODIFIED)
def analytics_script(request):
    script = ANALYTICS_SCRIPT_MAP[get_request_domain(request)]
    if not script:
        return HttpResponse(status=204)
    return HttpResponse(script, content_type='text/javascript', charset='utf-8')
