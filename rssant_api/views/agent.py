import os

from django.http import HttpResponse

from rssant.settings import BASE_DIR

AGENT_DOC_PATH = os.path.join(BASE_DIR, 'rssant_api', 'resources', 'agent.md')
DAILY_DIGEST_DOC_PATH = os.path.join(
    BASE_DIR, 'rssant_api', 'resources', 'daily-digest.md')


def _read_doc(path: str) -> str:
    with open(path, encoding='utf-8') as f:
        return f.read()


def _doc_response(request, path: str) -> HttpResponse:
    origin = request.build_absolute_uri('/').rstrip('/')
    content = _read_doc(path).replace('{origin}', origin)
    response = HttpResponse(content, content_type='text/markdown; charset=utf-8')
    response['Cache-Control'] = 'public, max-age=300'
    return response


def agent_doc(request):
    """公开的 AI 接入说明，不需要登录，只含只读信息和接口说明。"""
    return _doc_response(request, AGENT_DOC_PATH)


def daily_digest_doc(request):
    """公开的每日精选技能说明，给用户的 AI 读取，不包含用户数据。"""
    return _doc_response(request, DAILY_DIGEST_DOC_PATH)
