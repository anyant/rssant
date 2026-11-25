import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import rssant_worker.worker_service as worker_service
from rssant_common.dns_service import DNSService

FEED_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>RSSHub Test</title>
    <link>https://example.com</link>
    <description>test feed</description>
    <item>
      <title>story 1</title>
      <link>https://example.com/1</link>
      <guid>story-1</guid>
      <description>hello</description>
      <pubDate>Tue, 25 Nov 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

RSSHUB_URL = 'https://rsshub.app/v2ex/topics/hot'
ROUTE = '/v2ex/topics/hot'


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.paths.append(self.path)
        responses = self.server.responses.get(self.path)
        if not responses:
            status, headers, body = 404, {}, b'not found'
        elif len(responses) > 1:
            status, headers, body = responses.pop(0)
        else:
            status, headers, body = responses[0]
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _Server:
    def __init__(self, responses):
        self.httpd = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
        self.httpd.responses = responses
        self.httpd.paths = []
        self.thread = threading.Thread(target=self.httpd.serve_forever)
        self.thread.daemon = True

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc_info):
        self.httpd.shutdown()
        self.thread.join(timeout=3)

    @property
    def url(self):
        host, port = self.httpd.server_address
        return 'http://{}:{}'.format(host, port)

    @property
    def paths(self):
        return self.httpd.paths


def feed_response():
    return (200, {'Content-Type': 'application/rss+xml'}, FEED_XML)


class FakeServiceClient:
    def __init__(self, cooling=None):
        self.calls = []
        self.cooling = list(cooling or [])

    def call(self, api, data=None, timeout=None):
        self.calls.append((api, data))
        if api == 'harbor_rss.get_rsshub_cooling':
            return dict(urls=self.cooling)
        return None

    def calls_of(self, api):
        return [data for name, data in self.calls if name == api]


@pytest.fixture
def patched(monkeypatch):
    worker_service._rsshub_cooling_cache.update(time=0.0, urls=set())
    monkeypatch.setattr(worker_service, 'SERVICE_CLIENT', FakeServiceClient())
    monkeypatch.setattr(
        worker_service,
        'DNS_SERVICE',
        DNSService.create(allow_private_address=True),
    )
    return monkeypatch


def test_sync_rsshub_uses_second_instance_and_keeps_feed_url(patched):
    bad = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]})
    good = _Server({ROUTE: [feed_response()]})
    with bad, good:
        patched.setattr(worker_service, 'get_instances', lambda: [bad.url, good.url])
        worker_service.WorkerService().sync_feed(
            feed_id=0,
            url=RSSHUB_URL,
            use_proxy=False,
            checksum_data_base64=None,
            content_hash_base64=None,
            etag='W/"abc"',
            last_modified=None,
            is_refresh=False,
            rsshub_instance=None,
            rsshub_offset=0,
        )
    client = worker_service.SERVICE_CLIENT
    assert bad.paths == [ROUTE]
    assert good.paths == [ROUTE]
    updates = client.calls_of('harbor_rss.update_feed')
    assert len(updates) == 1
    feed = updates[0]['feed']
    # 命中镜像，但 Feed.url 仍是订阅自己的地址
    assert feed['url'] == RSSHUB_URL
    assert feed['rsshub_instance'] == good.url
    infos = client.calls_of('harbor_rss.update_feed_info')
    assert infos == []


def test_sync_rsshub_failure_advances_offset_without_cooling_http_errors(patched):
    bad = _Server({ROUTE: [(500, {'Content-Type': 'text/html'}, b'error')]})
    with bad:
        patched.setattr(worker_service, 'get_instances', lambda: [bad.url])
        worker_service.WorkerService().sync_feed(
            feed_id=0,
            url=RSSHUB_URL,
            use_proxy=False,
            checksum_data_base64=None,
            content_hash_base64=None,
            etag=None,
            last_modified=None,
            is_refresh=False,
            rsshub_instance=None,
            rsshub_offset=0,
        )
    client = worker_service.SERVICE_CLIENT
    infos = client.calls_of('harbor_rss.update_feed_info')
    assert len(infos) == 1
    data = infos[0]
    assert data['rsshub_offset'] == 3
    # HTTP 5xx 不冷却实例，只有传输层失败和 429 才冷却
    assert 'rsshub_cooling' not in data


def test_sync_rsshub_429_cools_instance(patched):
    server = _Server({ROUTE: [(429, {'Retry-After': '60'}, b'too many')]})
    with server:
        patched.setattr(worker_service, 'get_instances', lambda: [server.url])
        worker_service.WorkerService().sync_feed(
            feed_id=0,
            url=RSSHUB_URL,
            use_proxy=False,
            checksum_data_base64=None,
            content_hash_base64=None,
            etag=None,
            last_modified=None,
            is_refresh=False,
            rsshub_instance=None,
            rsshub_offset=0,
        )
    data = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.update_feed_info'
    )[0]
    assert data['rsshub_cooling'] == [
        dict(url=server.url, cooldown_seconds=60)
    ]


def test_sync_rsshub_all_cooling_short_circuits(patched):
    server = _Server({ROUTE: [feed_response()]})
    with server:
        instances = [server.url]
        patched.setattr(worker_service, 'get_instances', lambda: instances)
        worker_service.SERVICE_CLIENT.cooling = instances
        worker_service.WorkerService().sync_feed(
            feed_id=0,
            url=RSSHUB_URL,
            use_proxy=False,
            checksum_data_base64=None,
            content_hash_base64=None,
            etag=None,
            last_modified=None,
            is_refresh=False,
            rsshub_instance=None,
            rsshub_offset=0,
        )
        # 短路：一个请求都不发，且不推进 offset
        assert server.paths == []
    data = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.update_feed_info'
    )[0]
    assert 'rsshub_offset' not in data
    assert data['feed']['status'] == 'error'


def test_find_rsshub_success_uses_canonical_url(patched):
    good = _Server({ROUTE: [feed_response()]})
    with good:
        patched.setattr(worker_service, 'get_instances', lambda: [good.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=RSSHUB_URL
        )
    client = worker_service.SERVICE_CLIENT
    results = client.calls_of('harbor_rss.save_feed_creation_result')
    assert len(results) == 1
    result = results[0]
    assert result['retryable'] is False
    assert result['feed']['url'] == RSSHUB_URL
    assert result['feed']['rsshub_instance'] == good.url


def test_find_rsshub_all_404_is_permanent(patched):
    server = _Server({ROUTE: [(404, {'Content-Type': 'text/html'}, b'gone')]})
    with server:
        patched.setattr(worker_service, 'get_instances', lambda: [server.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=RSSHUB_URL
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed'] is None
    assert result['retryable'] is False
    assert '404' in result['messages'][0]


def test_find_rsshub_transient_failure_is_retryable(patched):
    server = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]})
    with server:
        patched.setattr(worker_service, 'get_instances', lambda: [server.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=RSSHUB_URL
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed'] is None
    assert result['retryable'] is True
    assert '已排入自动重试' in result['messages'][0]


def test_find_private_rsshub_falls_back_to_original_url(patched):
    bad = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]})
    original = _Server({ROUTE: [feed_response()]})
    private_url = original.url + ROUTE
    with bad, original:
        patched.setattr(worker_service, 'get_instances', lambda: [bad.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=private_url
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed']['url'] == private_url
    assert result['feed'].get('rsshub_instance') is None
