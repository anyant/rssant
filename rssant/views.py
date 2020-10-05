import json
from django.http import HttpResponse, JsonResponse
from django.utils import timezone
from django.views.decorators.http import condition
from django.views.decorators.cache import cache_control
from django.views.decorators.csrf import csrf_exempt
from shopant_sdk import ShopAntServer

from rssant_config import CONFIG
from rssant_common.changelog import ChangeLogList
from rssant_common.analytics import AnalyticsScript


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
CHANGE_LOGS_LAST_MODIFIED = timezone.now()
CHANGE_LOGS_ATOM = CHANGE_LOGS.to_atom()
CHANGE_LOGS_HTML = CHANGE_LOGS.to_html()


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: CHANGE_LOGS_LAST_MODIFIED)
def changelog_atom(request):
    return HttpResponse(
        CHANGE_LOGS_ATOM, content_type='text/xml', charset='utf-8')


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: CHANGE_LOGS_LAST_MODIFIED)
def changelog_html(request):
    return HttpResponse(
        CHANGE_LOGS_HTML, content_type='text/html', charset='utf-8')


ANALYTICS_SCRIPT = AnalyticsScript().generate()
ANALYTICS_SCRIPT_LAST_MODIFIED = timezone.now()


@cache_control(max_age=10 * 60)
@condition(last_modified_func=lambda r: ANALYTICS_SCRIPT_LAST_MODIFIED)
def analytics_script(request):
    if not ANALYTICS_SCRIPT:
        return HttpResponse(status=204)
    return HttpResponse(
        ANALYTICS_SCRIPT, content_type='text/javascript', charset='utf-8')


SHOPANT_SERVER = None
if CONFIG.shopant_product_id:
    SHOPANT_SERVER = ShopAntServer(
        product_id=CONFIG.shopant_product_id,
        product_secret=CONFIG.shopant_product_secret,
        url=CONFIG.shopant_url,
    )


@csrf_exempt
def shopant_integration(request):
    if request.method != 'POST':
        return HttpResponse(status=405)
    if not SHOPANT_SERVER:
        return HttpResponse(status=501)
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return HttpResponse('JSON reqired', status=400)
    result = SHOPANT_SERVER.integration(data)
    return JsonResponse(result, json_dumps_params={'ensure_ascii': False})
