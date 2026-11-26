import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import rssant_feedlib.rsshub as rsshub_module
import rssant_worker.worker_service as worker_service
from rssant_common.dns_service import DNSService
from rssant_worker.feed_creation_error import (
    ACCESS_DENIED,
    CONNECTION_ERROR,
    HTML_PAGE,
    NOT_A_FEED,
    NOT_FOUND,
)

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

EMPTY_FEED_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>RSSHub Empty</title>
    <link>https://example.com</link>
    <description>empty feed</description>
  </channel>
</rss>
"""

RSSHUB_URL = 'https://rsshub.app/v2ex/topics/hot'
ROUTE = '/v2ex/topics/hot'


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.paths.append(self.path)
        if getattr(self.server, 'delay', 0):
            time.sleep(self.server.delay)
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
    def __init__(self, responses, delay=0):
        self.httpd = ThreadingHTTPServer(('127.0.0.1', 0), _Handler)
        self.httpd.responses = responses
        self.httpd.paths = []
        self.httpd.delay = delay
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


def empty_feed_response():
    return (200, {'Content-Type': 'application/rss+xml'}, EMPTY_FEED_XML)


def html_response():
    return (200, {'Content-Type': 'text/html'}, b'<html>not a feed</html>')


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
    assert result['error_message'] == (
        'RSSHub 路由不存在（404），可能已经下线或改名。换一个 RSS 地址试试。'
    )


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
    assert 'error_message' not in result


class _PrivateHostConfig:
    """把 127.0.0.1 配成用户自建 RSSHub 主机，走私有分支"""

    rsshub_instances = ''
    rsshub_extra_hosts = '127.0.0.1'


def test_sync_rsshub_skips_empty_feed_and_tries_next_instance(patched):
    empty = _Server({ROUTE: [empty_feed_response()]})
    good = _Server({ROUTE: [feed_response()]})
    with empty, good:
        patched.setattr(worker_service, 'get_instances', lambda: [empty.url, good.url])
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
    assert empty.paths == [ROUTE]
    assert good.paths == [ROUTE]
    updates = worker_service.SERVICE_CLIENT.calls_of('harbor_rss.update_feed')
    assert updates[0]['feed']['rsshub_instance'] == good.url


def test_sync_rsshub_all_empty_feeds_is_failure(patched):
    empty = _Server({ROUTE: [empty_feed_response()]})
    with empty:
        patched.setattr(worker_service, 'get_instances', lambda: [empty.url])
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
    assert data['rsshub_offset'] == 3
    assert data['feed']['status'] == 'error'
    # 空内容不是“实例不可用”，文案要覆盖“没取到内容”
    assert '暂不可用' not in data['feed']['warnings']
    assert '取不到内容' in data['feed']['warnings']


def test_sync_rsshub_all_html_is_failure(patched):
    html = _Server({ROUTE: [html_response()]})
    with html:
        patched.setattr(worker_service, 'get_instances', lambda: [html.url])
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
    assert data['rsshub_offset'] == 3
    assert '取不到内容' in data['feed']['warnings']


def test_find_rsshub_skips_empty_and_html(patched):
    empty = _Server({ROUTE: [empty_feed_response()]})
    html = _Server({ROUTE: [html_response()]})
    good = _Server({ROUTE: [feed_response()]})
    with empty, html, good:
        patched.setattr(
            worker_service,
            'get_instances',
            lambda: [empty.url, html.url, good.url],
        )
        worker_service.WorkerService().find_feed(
            feed_creation_id=0, url=RSSHUB_URL
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed']['rsshub_instance'] == good.url


def test_find_rsshub_all_empty_is_retryable(patched):
    empty = _Server({ROUTE: [empty_feed_response()]})
    with empty:
        patched.setattr(worker_service, 'get_instances', lambda: [empty.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=RSSHUB_URL
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed'] is None
    assert result['retryable'] is True
    # 空内容要归到“没取到内容”，不能只说超时/限流/实例不可用
    assert '返回的不是 Feed' in result['messages'][0]


def test_find_rsshub_all_html_is_retryable(patched):
    html = _Server({ROUTE: [html_response()]})
    with html:
        patched.setattr(worker_service, 'get_instances', lambda: [html.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=RSSHUB_URL
        )
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed'] is None
    assert result['retryable'] is True
    assert '返回的不是 Feed' in result['messages'][0]


def test_find_private_rsshub_falls_back_to_original_url(patched):
    patched.setattr(rsshub_module, 'CONFIG', _PrivateHostConfig())
    bad = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]})
    original = _Server({ROUTE: [feed_response()]})
    private_url = original.url + ROUTE
    with bad, original:
        patched.setattr(worker_service, 'get_instances', lambda: [bad.url])
        worker_service.WorkerService().find_feed(
            feed_creation_id=7, url=private_url
        )
    client = worker_service.SERVICE_CLIENT
    # 先试池子里的实例（503），再试用户原地址（成功）
    assert bad.paths == [ROUTE]
    assert original.paths == [ROUTE]
    result = client.calls_of('harbor_rss.save_feed_creation_result')[0]
    assert result['feed']['url'] == private_url
    assert result['feed'].get('rsshub_instance') is None


def test_sync_private_rsshub_falls_back_to_original_url(patched):
    patched.setattr(rsshub_module, 'CONFIG', _PrivateHostConfig())
    bad = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]})
    original = _Server({ROUTE: [feed_response()]})
    private_url = original.url + ROUTE
    with bad, original:
        patched.setattr(worker_service, 'get_instances', lambda: [bad.url])
        worker_service.WorkerService().sync_feed(
            feed_id=0,
            url=private_url,
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
    assert bad.paths == [ROUTE]
    assert original.paths == [ROUTE]
    updates = client.calls_of('harbor_rss.update_feed')
    assert updates[0]['feed']['url'] == private_url
    assert updates[0]['feed'].get('rsshub_instance') is None


def test_find_rsshub_budget_exhausted_is_retryable(patched):
    # 两个慢实例：第一次尝试用完预算，第二次尝试前撞到 deadline，
    # 走 budget_exhausted 分支（默认池 6 个 × 6 秒 = 36 秒 < 40 秒预算，只有配置更多实例才会触发）
    slow_a = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]}, delay=0.5)
    slow_b = _Server({ROUTE: [(503, {'Content-Type': 'text/html'}, b'busy')]}, delay=0.5)
    with slow_a, slow_b:
        patched.setattr(
            worker_service, 'get_instances', lambda: [slow_a.url, slow_b.url]
        )
        patched.setattr(worker_service, 'FIND_BUDGET_SECONDS', 0.1)
        worker_service.WorkerService().find_feed(
            feed_creation_id=0, url=RSSHUB_URL
        )
    # 第一个实例发出请求，第二个因为预算用尽没有请求
    assert slow_a.paths == [ROUTE]
    assert slow_b.paths == []
    result = worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]
    assert result['feed'] is None
    assert result['retryable'] is True
    # 预算分支的总结文案，区别于普通的“超时、限流或实例不可用”
    assert '个实例后超时' in result['messages'][0]
    assert '已排入自动重试' in result['messages'][0]
    assert 'error_message' not in result


# ---- 普通 URL 查找失败：error_message ----

def _find_legacy(server_or_url):
    url = server_or_url.url + '/page' if hasattr(server_or_url, 'url') else server_or_url
    worker_service.WorkerService().find_feed(feed_creation_id=7, url=url)
    return worker_service.SERVICE_CLIENT.calls_of(
        'harbor_rss.save_feed_creation_result'
    )[0]


def test_find_legacy_404_sets_error_message(patched):
    server = _Server({})
    with server:
        result = _find_legacy(server)
    assert result['feed'] is None
    assert result['error_message'] == NOT_FOUND


def test_find_legacy_403_sets_error_message(patched):
    server = _Server({'/page': [(403, {'Content-Type': 'text/html'}, b'denied')]})
    with server:
        result = _find_legacy(server)
    assert result['error_message'] == ACCESS_DENIED


def test_find_legacy_html_sets_error_message(patched):
    server = _Server({'/page': [html_response()]})
    with server:
        result = _find_legacy(server)
    assert result['error_message'] == HTML_PAGE


def test_find_legacy_not_a_feed_sets_error_message(patched):
    server = _Server({'/page': [(200, {'Content-Type': 'text/plain'}, b'just text')]})
    with server:
        result = _find_legacy(server)
    assert result['error_message'] == NOT_A_FEED


def test_find_legacy_connection_error_sets_error_message(patched):
    # 借一个刚释放的本地端口，连接会被拒绝
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    result = _find_legacy('http://127.0.0.1:%d/page' % port)
    assert result['feed'] is None
    assert result['error_message'] == CONNECTION_ERROR


def test_find_legacy_success_has_no_error_message(patched):
    server = _Server({'/page': [feed_response()]})
    with server:
        result = _find_legacy(server)
    assert result['feed'] is not None
    assert 'error_message' not in result
