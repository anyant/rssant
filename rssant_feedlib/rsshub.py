"""RSSHub 地址识别、实例池写法和候选顺序。

识别只按 URL 现算，不在 Feed 上存标记：
- 公共实例（官方列表、社区实例、实测可用实例）只走实例池，不再请求原地址；
- 主机含 rsshub 但不在公共清单里的，按用户自建实例处理，保留最后一次原地址尝试；
- 其余地址不是 RSSHub，走原来的抓取逻辑。

候选顺序由 Feed.rsshub_offset 决定，每次抓取失败 offset += 3，相邻两次检查的
窗口不重叠，ceil(n/3) 次检查覆盖全部实例；粘性实例单独一次，不占窗口名额。
"""
from collections import namedtuple
from urllib.parse import urlsplit

from rssant_config import CONFIG

CANONICAL_BASE = 'https://rsshub.app'

# 本次实测能稳定出 Feed 的公共实例，按实测能力排序（见任务 45148 的附录 A）
DEFAULT_INSTANCES = (
    'https://rsshub.rss.tips',
    'https://rsshub.gneko.io',
    'https://rsshub.liumingye.cn',
    'https://rsshub-balancer.virworks.moe',
    'https://rss.wudifeixue.com',
    'https://rsshub.rssforever.com',
)

# 官方公共实例列表 + 线上日志里出现过的 RSSHub 主机；这些主机只走实例池
_PUBLIC_HOSTS = frozenset(
    """
    rsshub.app
    rsshub.rssforever.com
    hub.slarker.me
    rsshub.pseudoyu.com
    rsshub.rss.tips
    rsshub.ktachibana.party
    rss.owo.nz
    rss.wudifeixue.com
    rss.littlebaby.life
    rsshub.henry.wang
    holoxx.f5.si
    rsshub.umzzz.com
    rsshub.isrss.com
    rsshub.email-once.com
    rss.datuan.dev
    rss.4040940.xyz
    rsshub.cups.moe
    rss.spriple.org
    rsshub-balancer.virworks.moe
    rsshub.feeded.xyz
    rsshub.liumingye.cn
    rsshub-instance.zeabur.app
    rss.fatpandac.com
    rsshub.friesport.ac.cn
    rsshub.atgw.io
    rsshub.mubibai.com
    rsshub.woodland.cafe
    rsshub.aierliz.xyz
    rsshub.ioiox.com
    rsshub.gneko.io
    rsshub.raincandy.net
    rsshub.shunyu.wang
    rsshub.220303.xyz
    rsshub-ljp.herokuapp.com
    rsshub.moerats.com
    rsshub.kukmoon.com
    rsshub.uneasy.win
    rsshub.mystery0.app
    rsshub.zsliang.me
    """.split()
)

# 一轮抓取最多试几个实例：公共实例是粘性 1 + 窗口 3；自建实例是池子 2 + 原地址 1
MAX_ATTEMPTS_PUBLIC = 4
MAX_ATTEMPTS_PRIVATE = 3
WINDOW_SIZE = 3
OFFSET_STEP = 3

# 实例冷却：传输层失败固定 15 分钟，429 按 Retry-After，最长 30 分钟
COOLDOWN_SECONDS = 15 * 60
MAX_COOLDOWN_SECONDS = 30 * 60

# 单次尝试超时：定时抓取 10 秒；添加订阅 6 秒，总预算 40 秒
SYNC_ATTEMPT_TIMEOUT = 10
FIND_ATTEMPT_TIMEOUT = 6
FIND_BUDGET_SECONDS = 40


RsshubRoute = namedtuple('RsshubRoute', 'url host route is_public')
Candidate = namedtuple('Candidate', 'url instance')


def _get_config(config=None):
    return config if config is not None else CONFIG


def _config_value(config, name):
    if isinstance(config, dict):
        return config.get(name)
    return getattr(config, name, None)


def get_instances(config=None) -> list:
    """
    >>> get_instances({'rsshub_instances': 'https://a.example.com, https://b.example.com/'})
    ['https://a.example.com', 'https://b.example.com']
    """
    config = _get_config(config)
    raw = (_config_value(config, 'rsshub_instances') or '').strip()
    if not raw:
        return list(DEFAULT_INSTANCES)
    instances = []
    for item in raw.split(','):
        item = item.strip().rstrip('/')
        if not (item.startswith('http://') or item.startswith('https://')):
            continue
        # rsshub.app 对阅读器返回 403，只作规范地址标识，永远不发起请求
        if urlsplit(item).hostname == 'rsshub.app':
            continue
        instances.append(item)
    return instances or list(DEFAULT_INSTANCES)


def get_extra_hosts(config=None) -> set:
    config = _get_config(config)
    raw = (_config_value(config, 'rsshub_extra_hosts') or '').strip()
    return {item.strip().lower() for item in raw.split(',') if item.strip()}


def public_hosts(config=None) -> set:
    hosts = set(_PUBLIC_HOSTS)
    for instance in get_instances(config):
        host = urlsplit(instance).hostname
        if host:
            hosts.add(host.lower())
    return hosts


def _is_route_path(path: str) -> bool:
    if not path or path == '/':
        return False
    lower = path.lower()
    if lower.startswith('/api/') or lower.startswith('/assets/'):
        return False
    name = lower.rstrip('/').rsplit('/', 1)[-1]
    if name.endswith(('.js', '.css', '.png', '.jpg', '.svg', '.ico', '.map')):
        return False
    return True


def parse_rsshub_url(url, config=None):
    """
    >>> r = parse_rsshub_url('https://rsshub.app/v2ex/topics/hot')
    >>> r.route
    '/v2ex/topics/hot'
    >>> r.is_public
    True
    >>> parse_rsshub_url('https://example.com/feed.xml') is None
    True
    >>> parse_rsshub_url('https://my-rsshub.example.com/t66y/1').is_public
    False
    >>> parse_rsshub_url('https://rsshub.app/') is None
    True
    """
    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme not in ('http', 'https'):
        return None
    host = (parts.hostname or '').lower()
    if not host:
        return None
    path = parts.path or ''
    if not _is_route_path(path):
        return None
    route = path + (('?' + parts.query) if parts.query else '')
    if host in public_hosts(config):
        is_public = True
    elif host in get_extra_hosts(config) or 'rsshub' in host:
        is_public = False
    else:
        return None
    return RsshubRoute(url=url.strip(), host=host, route=route, is_public=is_public)


def route_of(url: str) -> str:
    """
    >>> route_of('https://rsshub.rss.tips/bilibili/ranking/0?limit=10')
    '/bilibili/ranking/0?limit=10'
    """
    parts = urlsplit(url)
    route = parts.path or '/'
    if parts.query:
        route += '?' + parts.query
    return route


def candidate_url(instance: str, route: str) -> str:
    """
    >>> candidate_url('https://rsshub.rss.tips/', '/v2ex/topics/hot')
    'https://rsshub.rss.tips/v2ex/topics/hot'
    """
    return instance.rstrip('/') + route


def canonical_url(route: str) -> str:
    """
    >>> canonical_url('/v2ex/topics/hot')
    'https://rsshub.app/v2ex/topics/hot'
    """
    return CANONICAL_BASE + route


def build_candidates(
    *,
    feed_id: int,
    offset: int,
    sticky: str = None,
    instances: list = None,
    cooling=None,
    route: str,
    is_public: bool,
    original_url: str = None,
) -> list:
    """
    公共实例：粘性 1 次 + 窗口 3 个，最多 4 次。
    自建实例：池子 2 个 + 原地址 1 次，最多 3 次。

    >>> [c.instance for c in build_candidates(
    ...     feed_id=0, offset=0, route='/x', is_public=True,
    ...     instances=['a', 'b', 'c', 'd', 'e', 'f'])]
    ['a', 'b', 'c']
    >>> [c.instance for c in build_candidates(
    ...     feed_id=0, offset=0, sticky='f', route='/x', is_public=True,
    ...     instances=['a', 'b', 'c', 'd', 'e', 'f'])]
    ['f', 'a', 'b', 'c']
    >>> [c.instance for c in build_candidates(
    ...     feed_id=1, offset=3, route='/x', is_public=True,
    ...     cooling={'e'}, instances=['a', 'b', 'c', 'd', 'e', 'f'])]
    ['f', 'a', 'b']
    """
    if not instances:
        return []
    cooling = cooling or set()
    n = len(instances)
    start = (feed_id + max(offset, 0)) % n

    def _window(limit: int) -> list:
        got = []
        for i in range(n):
            if len(got) >= limit:
                break
            instance = instances[(start + i) % n]
            if instance in got or instance in cooling:
                continue
            got.append(instance)
        return got

    if is_public:
        candidates = []
        if sticky and sticky in instances and sticky not in cooling:
            candidates.append(Candidate(candidate_url(sticky, route), sticky))
        for instance in _window(WINDOW_SIZE):
            if sticky and instance == sticky:
                continue
            candidates.append(Candidate(candidate_url(instance, route), instance))
        return candidates[:MAX_ATTEMPTS_PUBLIC]

    pool = []
    if sticky and sticky in instances and sticky not in cooling:
        pool.append(sticky)
    for instance in _window(2):
        if instance in pool:
            continue
        pool.append(instance)
        if len(pool) >= 2:
            break
    candidates = [Candidate(candidate_url(item, route), item) for item in pool[:2]]
    if original_url:
        candidates.append(Candidate(original_url, None))
    return candidates[:MAX_ATTEMPTS_PRIVATE]


def next_offset(offset: int) -> int:
    """
    >>> next_offset(0), next_offset(3)
    (3, 6)
    """
    return max(offset or 0, 0) + OFFSET_STEP
