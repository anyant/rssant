import requests
import pytest
import asyncio
from urllib.parse import urlparse
from pytest_httpserver import HTTPServer

from rssant_common.helper import aiohttp_client_session
from rssant_common.dns_service import DNSService, DNS_SERVICE, PrivateAddressError


def _requests_session():
    dns_service = DNSService.create(allow_private_address=False)
    session = requests.session()
    session.mount('http://', dns_service.requests_http_adapter())
    session.mount('https://', dns_service.requests_http_adapter())
    return session


@pytest.mark.xfail(run=False, reason='depends on test network')
@pytest.mark.parametrize('url', [
    'https://rsshub.app/',
    'http://www.baidu.com/'
])
def test_dns_service_urllib3(url):
    with _requests_session() as sess:
        assert sess.get(url).ok


def test_dns_service_urllib3_private(httpserver: HTTPServer):
    httpserver.expect_request('/200').respond_with_data('200')
    url = httpserver.url_for('/200')
    # expect OK when use standard requests Session
    assert requests.get(url).ok
    # expect raise PrivateAddressError when use DNSService
    with pytest.raises(PrivateAddressError):
        with _requests_session() as sess:
            sess.get(url)


async def _async_test_dns_service_aiohttp(url):
    resolver = DNS_SERVICE.aiohttp_resolver()
    async with aiohttp_client_session(resolver=resolver) as session:
        async with session.get(url) as resp:
            assert resp.status == 200


@pytest.mark.xfail(run=False, reason='depends on test network')
@pytest.mark.parametrize('url', [
    'https://rsshub.app/',
    'http://www.baidu.com/'
])
def test_dns_service_aiohttp(url):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_async_test_dns_service_aiohttp(url))


def _sync_async_resolve_host(dns_service: DNSService, host: str):
    loop = asyncio.get_event_loop()

    async def _resolve():
        resolver = dns_service.aiohttp_resolver(loop=loop)
        return (await resolver.resolve(host))[0]['host']

    return loop.run_until_complete(_resolve())


def _sync_resolve_host(dns_service: DNSService, host: str):
    return dns_service.resolve_urllib3(host)


@pytest.mark.parametrize('url, expect', [
    ('http://192.168.0.1:8080/', True),
    ('http://localhost:8080/', True),
    ('https://rsshub.app/', False),
    ('https://gitee.com/', False),
    ('http://www.baidu.com/', False),
])
@pytest.mark.parametrize('resolve', [
    _sync_async_resolve_host,
    _sync_resolve_host,
])
def test_resolve_private_address(resolve, url, expect):
    dns_service = DNSService.create(allow_private_address=False)
    host = urlparse(url).hostname
    try:
        resolve(dns_service, host)
    except PrivateAddressError:
        is_private = True
    else:
        is_private = False
    assert is_private == expect


@pytest.mark.xfail(run=False, reason='depends on test network')
def test_dns_service_refresh():
    DNS_SERVICE.refresh()
