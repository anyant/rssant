import os.path

from dotenv import load_dotenv
from validr import T, modelclass, fields, Invalid


@modelclass
class EnvConfig:
    debug = T.bool.default(True).desc('debug')
    root_url = T.url.default('http://127.0.0.1:6789')
    async_url_prefix = T.url.default('http://127.0.0.1:6786/api/v1')
    async_callback_url_prefix = T.url.default('http://127.0.0.1:6788/api/v1')
    secret_key = T.str.default('8k1v_4#kv4+3qu1=ulp+@@#65&++!fl1(e*7)ew&nv!)cq%e2y')
    allow_private_address = T.bool.default(False)
    check_feed_minutes = T.int.min(1).default(30)
    # postgres database
    pg_host = T.str.default('127.0.0.1').desc('postgres host')
    pg_port = T.int.default(5432).desc('postgres port')
    pg_db = T.str.default('rssant').desc('postgres database')
    pg_user = T.str.default('rssant').desc('postgres user')
    pg_password = T.str.default('rssant').desc('postgres password')
    # celery redis
    redis_url = T.str.default('redis://127.0.0.1:6379/0').desc('redis url')
    # github login
    github_client_id = T.str.optional
    github_secret = T.str.optional
    # sentry
    sentry_enable = T.bool.default(False)
    sentry_dsn = T.str.optional
    # celery sentry
    is_celery_process = T.bool.optional
    # email smtp
    smtp_enable = T.bool.default(False)
    smtp_host = T.str.optional
    smtp_port = T.int.min(0).optional
    smtp_username = T.str.optional
    smtp_password = T.str.optional
    smtp_use_ssl = T.bool.default(False)

    def __post_init__(self):
        if self.sentry_enable and not self.sentry_dsn:
            raise Invalid('sentry_dsn is required when sentry_enable=True')
        if self.smtp_enable:
            if not self.smtp_host:
                raise Invalid('smtp_host is required when smtp_enable=True')
            if not self.smtp_port:
                raise Invalid('smtp_port is required when smtp_enable=True')


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
