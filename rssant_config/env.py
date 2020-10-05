import os.path
import re

from dotenv import load_dotenv
from validr import T, modelclass, fields, Invalid

from rssant_common.validator import compiler
from actorlib.network_helper import LOCAL_NODE_NAME


validate_extra_networks = compiler.compile(T.list(T.dict(
    name=T.str,
    url=T.url.relaxed,
)))


@modelclass(compiler=compiler)
class ConfigModel:
    pass


class EnvConfig(ConfigModel):
    debug: bool = T.bool.default(False).desc('debug')
    profiler_enable: bool = T.bool.default(False).desc('enable profiler or not')
    debug_toolbar_enable: bool = T.bool.default(False).desc('enable debug toolbar or not')
    log_level: str = T.enum('DEBUG,INFO,WARNING,ERROR').default('INFO')
    root_url: str = T.url.relaxed.default('http://localhost:6789')
    scheduler_network: str = T.str.default('localhost')
    scheduler_url: str = T.url.relaxed.default('http://localhost:6790/api/v1/scheduler')
    scheduler_extra_networks: str = T.str.optional.desc('eg: name@url,name@url')
    secret_key: str = T.str.default('8k1v_4#kv4+3qu1=ulp+@@#65&++!fl1(e*7)ew&nv!)cq%e2y')
    allow_private_address: bool = T.bool.default(False)
    check_feed_minutes: int = T.int.min(1).default(30)
    feed_story_retention: int = T.int.min(1).default(5000).desc('max storys to keep per feed')
    pg_story_volumes: str = T.str.optional
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
    # analytics
    analytics_matomo_enable: bool = T.bool.default(False)
    analytics_matomo_url: str = T.str.optional
    analytics_matomo_site_id: str = T.str.optional
    analytics_google_enable: bool = T.bool.default(False)
    analytics_google_tracking_id: str = T.str.optional
    # shopant
    shopant_product_id: int = T.int.optional
    shopant_product_secret: str = T.str.optional
    shopant_url: str = T.url.optional

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
