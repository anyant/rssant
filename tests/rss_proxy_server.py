import pytest
import json
import logging
import traceback
from urllib.parse import urlparse, parse_qsl
from pytest_httpserver import HTTPServer
from werkzeug.datastructures import Headers as HTTPHeaders
from werkzeug import Response as WerkzeugResponse, Request as WerkzeugRequest
from rssant_common.dns_service import DNSService


LOG = logging.getLogger(__name__)


_RSS_PROXY_TOKEN = 'TEST_RSS_PROXY_TOKEN'


def _parse_query(qs) -> dict:
    query = {}
    for k, v in parse_qsl(qs):
        query[k] = v
    return query


def rss_proxy_handler(request: WerkzeugRequest) -> WerkzeugResponse:
    try:
        data = json.loads(request.data.decode('utf-8'))
        assert data['token'] == _RSS_PROXY_TOKEN
        assert data.get('method') in (None, 'GET', 'POST')
        url = urlparse(data['url'])
        query = _parse_query(url.query)
        assert url.path == '/not-proxy'
        assert HTTPHeaders(data['headers'])['user-agent']
    except Exception as ex:
        LOG.warning(ex, exc_info=ex)
        msg = traceback.format_exception_only(type(ex), ex)
        return WerkzeugResponse(msg, status=400)
    status = query.get('status')
    error = query.get('error')
    if error:
        if error == 'ERROR':
            headers = {'x-rss-proxy-status': 'ERROR'}
            return WerkzeugResponse(str(status), status=200, headers=headers)
        else:
            return WerkzeugResponse(str(status), status=int(error))
    else:
        status = int(status) if status else 200
        headers = {'x-rss-proxy-status': status}
        return WerkzeugResponse(str(status), status=200, headers=headers)


def _setup_rss_proxy(httpserver: HTTPServer):
    httpserver.expect_request("/rss-proxy", method='POST')\
        .respond_with_handler(rss_proxy_handler)
    httpserver.expect_request("/not-proxy").respond_with_data('ERROR', status=500)
    httpserver.expect_request("/direct/200").respond_with_data('DIRECT', status=200)
    proxy_url = httpserver.url_for('/rss-proxy')
    options = dict(
        dns_service=DNSService.create(allow_private_address=True),
        rss_proxy_url=proxy_url,
        rss_proxy_token=_RSS_PROXY_TOKEN,
    )
    return options


@pytest.fixture()
def rss_proxy_server(httpserver: HTTPServer):
    options = _setup_rss_proxy(httpserver)
    yield options
