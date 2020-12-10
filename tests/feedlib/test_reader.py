import asyncio
import os.path
import logging
from typing import Type
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer
from werkzeug import Response as WerkzeugResponse

from rssant_config import CONFIG
from rssant_feedlib.reader import FeedReader, FeedResponseStatus
from rssant_feedlib.async_reader import AsyncFeedReader
from rssant_common.dns_service import DNSService


LOG = logging.getLogger(__name__)


class SyncAsyncFeedReader:
    def __init__(self, *args, **kwargs):
        self._loop = asyncio.get_event_loop()
        self._loop_run = self._loop.run_until_complete
        self._reader = AsyncFeedReader(*args, **kwargs)

    @property
    def has_proxy(self):
        return self._reader.has_proxy

    def read(self, *args, **kwargs):
        return self._loop_run(self._reader.read(*args, **kwargs))

    def __enter__(self):
        self._loop_run(self._reader.__aenter__())
        return self

    def __exit__(self, *args):
        return self._loop_run(self._reader.__aexit__(*args))

    def close(self):
        return self._loop_run(self._reader.close())


def _build_proxy_options():
    if CONFIG.proxy_enable:
        yield 'proxy', dict(
            proxy_url=CONFIG.proxy_url,
        )
    if CONFIG.rss_proxy_enable:
        yield 'rss_proxy', dict(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )
    if CONFIG.proxy_enable and CONFIG.rss_proxy_enable:
        yield 'proxy_and_rss_proxy', dict(
            proxy_url=CONFIG.proxy_url,
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )


_PROXY_OPTION_IDS, _PROXY_OPTIONS = zip(*list(_build_proxy_options()))


@pytest.mark.xfail(run=False, reason='depends on test network')
@pytest.mark.parametrize('url', [
    'https://www.reddit.com/r/Python.rss',
    'https://www.youtube.com/feeds/videos.xml?channel_id=UCBcRF18a7Qf58cCRy5xuWwQ',
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
@pytest.mark.parametrize('proxy_config', _PROXY_OPTIONS, ids=_PROXY_OPTION_IDS)
def test_read_by_proxy(reader_class: Type[FeedReader], url, proxy_config):
    with reader_class(**proxy_config) as reader:
        response = reader.read(url, use_proxy=True)
    assert response.ok
    assert response.url == url


@pytest.mark.xfail(run=False, reason='depends on test network')
@pytest.mark.parametrize('url', [
    'https://www.ruanyifeng.com/blog/atom.xml',
    'https://blog.guyskk.com/feed.xml',
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_by_real(reader_class: Type[FeedReader], url):
    with reader_class() as reader:
        response = reader.read(url)
    assert response.ok
    assert response.url == url


@pytest.mark.parametrize('status', [
    200, 201, 301, 302, 400, 403, 404, 500, 502, 600,
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_status(reader_class: Type[FeedReader], httpserver: HTTPServer, status: int):
    dns_service = DNSService.create(allow_private_address=True)
    options = dict(allow_non_webpage=True, dns_service=dns_service)
    local_resp = WerkzeugResponse(str(status), status=status)
    httpserver.expect_request("/status").respond_with_response(local_resp)
    url = httpserver.url_for("/status")
    with reader_class(**options) as reader:
        response = reader.read(url)
        assert response.status == status
        assert response.content == str(status).encode()


@pytest.mark.parametrize('mime_type', [
    'image/png', 'text/csv',
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_non_webpage(reader_class: Type[FeedReader], httpserver: HTTPServer, mime_type: str):
    options = dict(dns_service=DNSService.create(allow_private_address=True))
    local_resp = WerkzeugResponse(b'xxxxxxxx', mimetype=mime_type)
    httpserver.expect_request("/non-webpage").respond_with_response(local_resp)
    url = httpserver.url_for("/non-webpage")
    with reader_class(**options) as reader:
        response = reader.read(url)
        assert response.status == FeedResponseStatus.CONTENT_TYPE_NOT_SUPPORT_ERROR
        assert not response.content


@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_private_addres(reader_class: Type[FeedReader], httpserver: HTTPServer):
    httpserver.expect_request("/private-address").respond_with_json(0)
    url = httpserver.url_for("/private-address")
    dns_service = DNSService.create(allow_private_address=False)
    with reader_class(dns_service=dns_service) as reader:
        response = reader.read(url)
        assert response.status == FeedResponseStatus.PRIVATE_ADDRESS_ERROR
        assert not response.content


_data_dir = Path(__file__).parent / 'testdata'


def _collect_testdata_filepaths():
    cases = []
    for filepath in (_data_dir / 'encoding/chardet').glob("*"):
        cases.append(filepath.absolute())
    for filepath in (_data_dir / 'parser').glob("*/*"):
        cases.append(filepath.absolute())
    cases = [os.path.relpath(x, _data_dir) for x in cases]
    return cases


def _collect_header_cases():
    return [
        "application/json;charset=utf-8",
        "application/atom+xml; charset='us-ascii'",
        "application/atom+xml; charset='gb2312'",
        "application/atom+xml;CHARSET=GBK",
        None,
    ]


@pytest.mark.parametrize('filepath', _collect_testdata_filepaths())
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_testdata(reader_class: Type[FeedReader], httpserver: HTTPServer, filepath: str):
    filepath = _data_dir / filepath
    content = filepath.read_bytes()
    urls = []
    for i, x in enumerate(_collect_header_cases()):
        local_resp = WerkzeugResponse(content, content_type=x)
        httpserver.expect_request(f"/testdata/{i}").respond_with_response(local_resp)
        urls.append(httpserver.url_for(f"/testdata/{i}"))
    options = dict(dns_service=DNSService.create(allow_private_address=True))
    with reader_class(**options) as reader:
        for url in urls:
            response = reader.read(url)
            assert response.ok
            assert response.content == content
            assert response.encoding
            assert response.feed_type


@pytest.mark.parametrize('status', [
    200, 201, 301, 302, 400, 403, 404, 500, 502, 600,
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_rss_proxy(reader_class: Type[FeedReader], rss_proxy_server, httpserver: HTTPServer, status: int):
    options = rss_proxy_server
    url = httpserver.url_for('/not-proxy')
    with reader_class(**options) as reader:
        response = reader.read(url + f'?status={status}', use_proxy=True)
        httpserver.check_assertions()
        assert response.status == status


@pytest.mark.parametrize('error', [
    301, 302, 400, 403, 404, 500, 502, 'ERROR',
])
@pytest.mark.parametrize('reader_class', [FeedReader, SyncAsyncFeedReader])
def test_read_rss_proxy_error(reader_class: Type[FeedReader], rss_proxy_server, httpserver: HTTPServer, error):
    options = rss_proxy_server
    url = httpserver.url_for('/not-proxy')
    with reader_class(**options) as reader:
        response = reader.read(url + f'?error={error}', use_proxy=True)
        httpserver.check_assertions()
        assert response.status == FeedResponseStatus.RSS_PROXY_ERROR
