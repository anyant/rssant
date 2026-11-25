from rssant_feedlib.rsshub import (
    DEFAULT_INSTANCES,
    build_candidates,
    candidate_url,
    canonical_url,
    get_extra_hosts,
    get_instances,
    next_offset,
    parse_rsshub_url,
    route_of,
)


def test_parse_public_instance():
    route = parse_rsshub_url('https://rsshub.app/github/trending/daily/any')
    assert route.is_public
    assert route.route == '/github/trending/daily/any'
    assert canonical_url(route.route) == (
        'https://rsshub.app/github/trending/daily/any'
    )


def test_parse_keeps_query_and_atom_suffix():
    route = parse_rsshub_url(
        'https://rsshub.rss.tips/weibo/user/1.atom?readable=1&limit=10'
    )
    assert route.route == '/weibo/user/1.atom?readable=1&limit=10'


def test_parse_private_instance():
    route = parse_rsshub_url('https://my-rsshub.example.com/t66y/1')
    assert not route.is_public


def test_parse_ignores_non_rsshub_and_homepage():
    assert parse_rsshub_url('https://example.com/feed.xml') is None
    assert parse_rsshub_url('https://blog.example.com/') is None
    assert parse_rsshub_url('https://rsshub.app/') is None
    assert parse_rsshub_url('https://rsshub.app/assets/app.js') is None
    assert parse_rsshub_url('ftp://rsshub.app/v2ex/topics/hot') is None


def test_parse_extra_hosts_are_private():
    config = dict(rsshub_extra_hosts='rss.example.com')
    route = parse_rsshub_url('https://rss.example.com/feed', config=config)
    assert route is not None
    assert not route.is_public


def test_get_instances_config():
    assert get_instances({}) == list(DEFAULT_INSTANCES)
    config = dict(rsshub_instances='https://a.example.com/, https://b.example.com')
    assert get_instances(config) == [
        'https://a.example.com',
        'https://b.example.com',
    ]
    assert get_extra_hosts(dict(rsshub_extra_hosts='A.example.com, b.example.com')) == {
        'a.example.com',
        'b.example.com',
    }


def test_get_instances_ignores_rsshub_app():
    # 官方站对阅读器 403，即使配进来也不作为抓取实例
    config = dict(rsshub_instances='https://rsshub.app, https://a.example.com')
    assert get_instances(config) == ['https://a.example.com']


def test_candidate_url_strips_trailing_slash():
    assert candidate_url('https://a.example.com/', '/x') == 'https://a.example.com/x'


def test_route_of():
    assert route_of('https://rsshub.app/x/y?z=1') == '/x/y?z=1'


def test_public_candidates_sticky_and_window():
    instances = ['a', 'b', 'c', 'd', 'e', 'f']
    candidates = build_candidates(
        feed_id=0,
        offset=0,
        sticky='e',
        route='/x',
        is_public=True,
        instances=instances,
    )
    # 粘性第一个，窗口 a,b,c 完整保留，不被粘性占掉名额
    assert [c.instance for c in candidates] == ['e', 'a', 'b', 'c']
    assert candidates[0].url == 'e/x'


def test_public_candidates_skip_cooling():
    candidates = build_candidates(
        feed_id=0,
        offset=0,
        route='/x',
        is_public=True,
        instances=['a', 'b', 'c', 'd'],
        cooling={'a'},
    )
    assert [c.instance for c in candidates] == ['b', 'c', 'd']


def test_public_candidates_all_cooling_is_empty():
    # 全部冷却时返回空，调用方短路返回且不推进 offset
    candidates = build_candidates(
        feed_id=0,
        offset=0,
        route='/x',
        is_public=True,
        instances=['a', 'b'],
        cooling={'a', 'b'},
    )
    assert candidates == []


def test_private_candidates_two_pool_plus_original():
    candidates = build_candidates(
        feed_id=0,
        offset=0,
        route='/x',
        is_public=False,
        instances=['a', 'b', 'c', 'd'],
        original_url='https://my-rsshub.example.com/x',
    )
    assert [c.instance for c in candidates] == ['a', 'b', None]
    assert candidates[-1].url == 'https://my-rsshub.example.com/x'


def test_offset_step():
    assert next_offset(0) == 3
    assert next_offset(None) == 3
    assert next_offset(3) == 6


def test_rotation_covers_all_instances():
    # 每次失败 offset 加 3：任意起点下，ceil(n/3) 次检查覆盖全部实例
    instances = list('abcdef')
    n = len(instances)
    for feed_id in range(n):
        seen = set()
        offset = 0
        for _ in range(-(-n // 3)):
            candidates = build_candidates(
                feed_id=feed_id,
                offset=offset,
                route='/x',
                is_public=True,
                instances=instances,
            )
            seen.update(c.instance for c in candidates)
            offset = next_offset(offset)
        assert seen == set(instances), (feed_id, seen)
