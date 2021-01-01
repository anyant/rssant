import time
import logging
from django.http import HttpResponse, HttpRequest


LOG = logging.getLogger(__name__)


class RssantTimerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        t_begin = time.monotonic()
        response = self.get_response(request)
        cost_ms = round((time.monotonic() - t_begin) * 1000)
        LOG.info(f'X-Time: {cost_ms}ms')
        response['X-Time'] = f'{cost_ms}ms'
        return response
