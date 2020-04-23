import requests
import pytest
import asyncio
from pytest_httpserver import HTTPServer

from rssant_common.helper import aiohttp_client_session
from rssant_common.dns_service import DNS_SERVICE


@pytest.mark.xfail(run=False, reason='depends on test network')
def test_dns_service_urllib3():
    url = 'https://rsshub.app/'
    assert requests.get(url).ok


def test_dns_service_direct(httpserver: HTTPServer):
    httpserver.expect_request('/200').respond_with_data('200')
    url = httpserver.url_for('/200')
    assert requests.get(url).ok


async def _async_test_dns_service_aiohttp():
    url = 'https://rsshub.app/'
    resolver = DNS_SERVICE.aiohttp_resolver()
    async with aiohttp_client_session(resolver=resolver) as session:
        resp = await session.get(url)
        assert resp.status == 200


@pytest.mark.xfail(run=False, reason='depends on test network')
def test_dns_service_aiohttp():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_async_test_dns_service_aiohttp())


@pytest.mark.xfail(run=False, reason='depends on test network')
def test_dns_service_refresh():
    DNS_SERVICE.refresh()
