import time
from django.http import HttpRequest, HttpResponse
from prometheus_client import Histogram
from prometheus_client.exposition import choose_encoder, REGISTRY


def django_metrics_view(request: HttpRequest) -> HttpResponse:
    registry = REGISTRY
    accept = request.headers.get('Accept')
    encoder, content_type = choose_encoder(accept)
    if 'name[]' in request.GET:
        name = request.GET['name[]']
        registry = registry.restricted_registry(name)
    output = encoder(registry)
    return HttpResponse(content=output, content_type=content_type)


API_TIME = Histogram(
    'rssant_api_time', 'api execute time', [
        'path', 'method', 'status'
    ],
    buckets=(
        .010, .025, .050, .075, .100, .150, .250, .350, .500,
        .750, 1.0, 1.5, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0,
    )
)


class RssantPrometheusMiddleware:
    """
    https://github.com/korfuri/django-prometheus/blob/master/django_prometheus/middleware.py
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest):
        t_begin = time.monotonic()
        response = self.get_response(request)
        cost = time.monotonic() - t_begin
        path_name = self._get_path_name(request)
        status = str(response.status_code)
        API_TIME.labels(path_name, request.method, status).observe(cost)
        return response

    def _get_path_name(self, request):
        path_name = "<unnamed_path>"
        if hasattr(request, "resolver_match"):
            if request.resolver_match is not None:
                # resolver_match.route requires django 2.2+
                if request.resolver_match.route is not None:
                    path_name = request.resolver_match.route
        return path_name
