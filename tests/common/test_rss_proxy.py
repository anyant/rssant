from pytest_httpserver import HTTPServer

from rssant_common.rss_proxy import RSSProxyClient, ProxyStrategy


def test_rss_proxy_direct_first(rss_proxy_server, httpserver: HTTPServer):
    options = rss_proxy_server
    direct_url = httpserver.url_for('/direct/200')
    not_proxy_url = httpserver.url_for('/not-proxy?status=200')
    client = RSSProxyClient(
        rss_proxy_url=options['rss_proxy_url'],
        rss_proxy_token=options['rss_proxy_token'],
    )
    res = client.request('GET', direct_url)
    assert res.status_code == 200
    assert res.text == 'DIRECT'
    # direct request /not-proxy will response 500
    assert client.request('GET', not_proxy_url).status_code == 500


def test_rss_proxy_proxy_first(rss_proxy_server, httpserver: HTTPServer):
    options = rss_proxy_server
    direct_url = httpserver.url_for('/direct/200')
    not_proxy_url = httpserver.url_for('/not-proxy?status=200')
    client = RSSProxyClient(
        rss_proxy_url=options['rss_proxy_url'],
        rss_proxy_token=options['rss_proxy_token'],
        proxy_strategy=ProxyStrategy.PROXY_FIRST,
    )
    # rss proxy failed, fallback to direct
    res = client.request('GET', direct_url)
    assert res.status_code == 200
    assert res.text == 'DIRECT'
    # rss proxy success
    assert client.request('GET', not_proxy_url).status_code == 200
