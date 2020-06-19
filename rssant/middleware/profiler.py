import time
import logging
from collections import OrderedDict

from pyinstrument import Profiler
from django.http import HttpResponse, HttpRequest


LOG = logging.getLogger(__name__)

_PROFILER_RECORDS = OrderedDict()


class RssantProfilerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest):
        response = self._render_profiler_record(request)
        if response:
            return response
        profiler = None
        try:
            profiler = Profiler()
            profiler.start()
            response = self.get_response(request)
        finally:
            if profiler is not None:
                profiler.stop()
                print(profiler.output_text(unicode=True, color=True))
                link = self._output_html(request, profiler)
                print(f'* Profiler HTML: {link}\n')
        return response

    def _output_html(self, request: HttpRequest, profiler: Profiler):
        html = profiler.output_html()
        t = int(time.time() * 1000)
        key = '{}-{}-{}'.format(t, request.method, request.path)
        _PROFILER_RECORDS[key] = html
        while len(_PROFILER_RECORDS) > 20:
            _PROFILER_RECORDS.popitem(False)
        port = request.META['SERVER_PORT']
        link = f'http://localhost:{port}/__profiler__/{key}'
        return link

    def _render_profiler_record(self, request: HttpRequest):
        prefix = '/__profiler__/'
        if not request.path.startswith(prefix):
            return None
        key = request.path[len(prefix):]
        html = _PROFILER_RECORDS.get(key)
        if not html:
            return None
        content = html.encode('utf-8')
        return HttpResponse(content, content_type='text/html', charset='utf-8')
