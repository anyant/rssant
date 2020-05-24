import threading
import time
from contextlib import contextmanager

from debug_toolbar.panels import Panel


_seaweed_metrics = threading.local()


def _init_seaweed_metrics():
    _seaweed_metrics.value = {
        'get': 0,
        'get_time': 0.0,
        'put': 0,
        'put_time': 0.0,
        'delete': 0,
        'delete_time': 0.0,
    }


def _close_seaweed_metrics():
    delattr(_seaweed_metrics, 'value')


class SeaweedMetrics:

    GET = 'get'
    PUT = 'put'
    DELETE = 'delete'

    @classmethod
    @contextmanager
    def record(cls, op: str, n: int = 1):
        if op not in (cls.GET, cls.PUT, cls.DELETE):
            raise ValueError(f'unknown seaweed operation {op!r}')
        if getattr(_seaweed_metrics, 'value', None) is None:
            _init_seaweed_metrics()
        t_begin = time.time()
        try:
            yield
        finally:
            cost = time.time() - t_begin
            _seaweed_metrics.value[op] += n
            _seaweed_metrics.value[f'{op}_time'] += cost


class SeaweedPanel(Panel):

    has_content = False

    def enable_instrumentation(self):
        _init_seaweed_metrics()

    def process_request(self, request):
        response = super().process_request(request)
        stats = dict(_seaweed_metrics.value)
        self.record_stats(stats)
        return response

    def disable_instrumentation(self):
        _close_seaweed_metrics()
