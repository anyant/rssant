import os

from django.http import HttpResponse

from rssant.settings import BASE_DIR

AGENT_DOC_PATH = os.path.join(BASE_DIR, 'rssant_api', 'resources', 'agent.md')


def _read_agent_doc() -> str:
    with open(AGENT_DOC_PATH, encoding='utf-8') as f:
        return f.read()


def agent_doc(request):
    """公开的 AI 接入说明，不需要登录，只含只读信息和接口说明。"""
    origin = request.build_absolute_uri('/').rstrip('/')
    content = _read_agent_doc().replace('{origin}', origin)
    response = HttpResponse(content, content_type='text/markdown; charset=utf-8')
    response['Cache-Control'] = 'public, max-age=300'
    return response
