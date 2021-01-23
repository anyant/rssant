from django.http import HttpRequest
from rssant_config import CONFIG


def get_request_domain(request: HttpRequest) -> str:
    request_host = request and request.headers.get('x-rssant-host')
    if not request_host:
        request_host = request and request.get_host()
    if request_host:
        request_domain = request_host.split(':')[0]
        if request_domain in CONFIG.standby_domain_set:
            return request_domain
    return CONFIG.root_domain


def get_request_root_url(request: HttpRequest) -> str:
    domain = get_request_domain(request)
    return CONFIG.root_url.replace(CONFIG.root_domain, domain)
