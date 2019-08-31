import os.path

from dotenv import load_dotenv
from validr import T, modelclass, fields, Invalid

from rssant_common.validator import compiler
from actorlib.network_helper import LOCAL_NODE_NAME


validate_extra_networks = compiler.compile(T.list(T.dict(
    name=T.str,
    url=T.str,  # TODO: url
)))


@modelclass
class EnvConfig:
    debug = T.bool.default(True).desc('debug')
    root_url = T.url.default('http://localhost:6789')
    async_url_prefix = T.url.default('http://localhost:6786/api/v1')
    async_callback_url_prefix = T.url.default('http://localhost:6788/api/v1')
    scheduler_network = T.str.default('localhost')
    scheduler_url = T.url.default('http://localhost:6790/api/v1/scheduler')
    scheduler_extra_networks = T.str.optional.desc('eg: name@url,name@url')
    secret_key = T.str.default('8k1v_4#kv4+3qu1=ulp+@@#65&++!fl1(e*7)ew&nv!)cq%e2y')
    allow_private_address = T.bool.default(False)
    check_feed_minutes = T.int.min(1).default(30)
    # actor
    actor_storage_path = T.str.optional
    actor_storage_max_pending_size = T.int.min(0).default(500)
    actor_storage_max_done_size = T.int.min(0).default(5000)
    actor_storage_compact_interval = T.int.min(1).default(60)
    actor_ack_timeout = T.int.min(1).default(600)
    actor_max_retry_count = T.int.min(0).default(3)
    actor_token = T.str.optional
    # postgres database
    pg_host = T.str.default('localhost').desc('postgres host')
    pg_port = T.int.default(5432).desc('postgres port')
    pg_db = T.str.default('rssant').desc('postgres database')
    pg_user = T.str.default('rssant').desc('postgres user')
    pg_password = T.str.default('rssant').desc('postgres password')
    # celery redis
    redis_url = T.str.default('redis://localhost:6379/0').desc('redis url')
    # github login
    github_client_id = T.str.optional
    github_secret = T.str.optional
    # sentry
    sentry_enable = T.bool.default(False)
    sentry_dsn = T.str.optional
    # celery sentry
    is_celery_process = T.bool.optional
    # email smtp
    admin_email = T.email.default('admin@localhost.com')
    smtp_enable = T.bool.default(False)
    smtp_host = T.str.optional
    smtp_port = T.int.min(0).optional
    smtp_username = T.str.optional
    smtp_password = T.str.optional
    smtp_use_ssl = T.bool.default(False)

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
