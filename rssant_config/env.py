import os.path
import re
from functools import cached_property
from urllib.parse import urlparse

from dotenv import load_dotenv
from validr import Compiler, Invalid, T, fields, modelclass

from rssant_common.network_helper import LOCAL_NODE_NAME

MAX_FEED_COUNT = 5000


compiler = Compiler()
validate_extra_networks = compiler.compile(T.list(T.dict(
    name=T.str,
    url=T.url,
)))


@modelclass(compiler=compiler)
class ConfigModel:
    pass


class GitHubConfigModel(ConfigModel):
    domain: str = T.str
    client_id: str = T.str
    secret: str = T.str


class EnvConfig(ConfigModel):
    debug: bool = T.bool.default(False).desc('debug')
    profiler_enable: bool = T.bool.default(False).desc('enable profiler or not')
    debug_toolbar_enable: bool = T.bool.default(False).desc('enable debug toolbar or not')
    log_level: str = T.enum('DEBUG,INFO,WARNING,ERROR').default('INFO')
    root_url: str = T.url.default('http://localhost:6789')
    standby_domains: str = T.str.optional
    scheduler_network: str = T.str.default('localhost')
    scheduler_url: str = T.url.default('http://localhost:6790/api/v1/scheduler')
    scheduler_extra_networks: str = T.str.optional.desc('eg: name@url,name@url')
    secret_key: str = T.str.default('8k1v_4#kv4+3qu1=ulp+@@#65&++!fl1(e*7)ew&nv!)cq%e2y')
    allow_private_address: bool = T.bool.default(False)
    check_feed_minutes: int = T.int.min(1).default(30)
    feed_story_retention: int = T.int.min(1).default(5000).desc('max storys to keep per feed')
    pg_story_volumes: str = T.str.optional
    feed_reader_request_timeout: int = T.int.default(90).desc('feed reader request timeout')
    # actor
    actor_storage_path: str = T.str.default('data/actor_storage')
    actor_storage_compact_wal_delta: int = T.int.min(1).default(5000)
    actor_queue_max_complete_size: int = T.int.min(0).default(500)
    actor_max_retry_time: int = T.int.min(1).default(600)
    actor_max_retry_count: int = T.int.min(0).default(1)
    actor_token: str = T.str.optional
    # postgres database
    pg_host: str = T.str.default('localhost').desc('postgres host')
    pg_port: int = T.int.default(5432).desc('postgres port')
    pg_db: str = T.str.default('rssant').desc('postgres database')
    pg_user: str = T.str.default('rssant').desc('postgres user')
    pg_password: str = T.str.default('rssant').desc('postgres password')
    # github login
    github_client_id: str = T.str.optional
    github_secret: str = T.str.optional
    github_standby_configs: str = T.str.optional.desc('domain,client_id,secret;')
    # sentry
    sentry_enable: bool = T.bool.default(False)
    sentry_dsn: str = T.str.optional
    # email smtp
    admin_email: str = T.email.default('admin@localhost.com')
    smtp_enable: bool = T.bool.default(False)
    smtp_host: str = T.str.optional
    smtp_port: int = T.int.min(0).optional
    smtp_username: str = T.str.optional
    smtp_password: str = T.str.optional
    smtp_use_ssl: bool = T.bool.default(False)
    # rss proxy
    rss_proxy_url: str = T.url.optional
    rss_proxy_token: str = T.str.optional
    rss_proxy_enable: bool = T.bool.default(False)
    # http proxy or socks proxy
    proxy_url: str = T.url.scheme('http https socks5').optional
    proxy_enable: bool = T.bool.default(False)
    # analytics matomo
    analytics_matomo_enable: bool = T.bool.default(False)
    analytics_matomo_url: str = T.str.optional
    analytics_matomo_site_id: str = T.str.optional
    # analytics google
    analytics_google_enable: bool = T.bool.default(False)
    analytics_google_tracking_id: str = T.str.optional
    # analytics plausible
    analytics_plausible_enable: str = T.bool.default(False)
    analytics_plausible_url: str = T.str.optional
    analytics_plausible_domain: str = T.str.optional
    # ezrevenue
    ezrevenue_enable: bool = T.bool.default(False)
    ezrevenue_project_id: str = T.str.optional
    ezrevenue_project_secret: str = T.str.optional
    ezrevenue_base_url: str = T.url.optional
    # image proxy
    image_proxy_enable: bool = T.bool.default(True)
    image_proxy_urls: bool = T.str.default('origin').desc('逗号分隔的URL列表')
    image_token_secret: str = T.str.default('rssant')
    image_token_expires: float = T.timedelta.min('1s').default('30m')
    detect_story_image_enable: bool = T.bool.default(False)
    # hashid salt
    hashid_salt: str = T.str.default('rssant')

    def _parse_scheduler_extra_networks(self):
        if not self.scheduler_extra_networks:
            return []
        networks = []
        for part in self.scheduler_extra_networks.strip().split(','):
            part = part.split('@', maxsplit=1)
            if len(part) != 2:
                raise Invalid('invalid scheduler_extra_networks')
            name, url = part
            networks.append(dict(name=name, url=url))
        networks = validate_extra_networks(networks)
        return list(networks)

    @classmethod
    def _parse_story_volumes(cls, text: str):
        """
        Format:
            {volume}:{user}:{password}@{host}:{port}/{db}/{table}

        >>> volumes = EnvConfig._parse_story_volumes('0:user:password@host:5432/db/table')
        >>> expect = {0: dict(
        ...    user='user', password='password',
        ...    host='host', port=5432, db='db', table='table'
        ... )}
        >>> volumes == expect
        True
        """
        re_volume = re.compile(
            r'^(\d+)\:([^:@/]+)\:([^:@/]+)\@([^:@/]+)\:(\d+)\/([^:@/]+)\/([^:@/]+)$')
        volumes = {}
        for part in text.split(','):
            match = re_volume.match(part)
            if not match:
                raise Invalid(f'invalid story volume {part!r}')
            volume = int(match.group(1))
            volumes[volume] = dict(
                user=match.group(2),
                password=match.group(3),
                host=match.group(4),
                port=int(match.group(5)),
                db=match.group(6),
                table=match.group(7),
            )
        return volumes

    def _parse_github_standby_configs(self):
        configs = {}
        items = (self.github_standby_configs or '').strip().split(';')
        for item in filter(None, items):
            parts = item.split(',')
            if len(parts) != 3:
                raise Invalid('invalid github standby configs')
            domain, client_id, secret = parts
            configs[domain] = GitHubConfigModel(
                domain=domain, client_id=client_id, secret=secret)
        return configs

    def __post_init__(self):
        if self.sentry_enable and not self.sentry_dsn:
            raise Invalid('sentry_dsn is required when sentry_enable=True')
        if self.smtp_enable:
            if not self.smtp_host:
                raise Invalid('smtp_host is required when smtp_enable=True')
            if not self.smtp_port:
                raise Invalid('smtp_port is required when smtp_enable=True')
        scheduler_extra_networks = self._parse_scheduler_extra_networks()
        self.registery_node_spec = {
            'name': 'scheduler',
            'modules': ['scheduler'],
            'networks': [{
                'name': self.scheduler_network,
                'url': self.scheduler_url,
            }] + scheduler_extra_networks
        }
        self.current_node_spec = {
            'name': '{}@{}'.format(LOCAL_NODE_NAME, os.getpid()),
            'modules': [],
            'networks': [{
                'name': self.scheduler_network,
                'url': None,
            }]
        }
        if self.pg_story_volumes:
            volumes = self._parse_story_volumes(self.pg_story_volumes)
        else:
            volumes = {0: dict(
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port,
                db=self.pg_db,
                table='story_volume_0',
            )}
        self.pg_story_volumes_parsed = volumes
        self.github_standby_configs_parsed = self._parse_github_standby_configs()

    @cached_property
    def root_domain(self) -> str:
        return urlparse(self.root_url).hostname

    @cached_property
    def standby_domain_set(self) -> set:
        return set((self.standby_domains or '').strip().split(','))

    @cached_property
    def image_proxy_url_list(self) -> list:
        url_s = (self.image_proxy_urls or '').strip().split(',')
        return list(sorted(set(url_s)))


def load_env_config() -> EnvConfig:
    envfile_path = os.getenv('RSSANT_CONFIG')
    if envfile_path:
        envfile_path = os.path.abspath(os.path.expanduser(envfile_path))
        print(f'* Load envfile at {envfile_path}')
        load_dotenv(envfile_path)
    configs = {}
    for name in fields(EnvConfig):
        key = ('RSSANT_' + name).upper()
        configs[name] = os.environ.get(key, None)
    return EnvConfig(configs)


CONFIG = load_env_config()
