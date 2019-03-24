import os
from validr import T, modelclass, fields


@modelclass
class EnvConfig:

    debug = T.bool.default(True).desc('debug')
    secret_key = T.str.default('8k1v_4#kv4+3qu1=ulp+@@#65&++!fl1(e*7)ew&nv!)cq%e2y')
    # postgres database
    pg_host = T.str.default('127.0.0.1').desc('postgres host')
    pg_port = T.int.default(5432).desc('postgres port')
    pg_db = T.str.default('rssant').desc('postgres database')
    pg_user = T.str.default('rssant').desc('postgres user')
    pg_password = T.str.default('rssant').desc('postgres password')
    # celery redis
    redis_url = T.str.default('redis://127.0.0.1:6379/0').desc('redis url')
    # github login
    github_client_id = T.str.default('a30a7a62fd4a648c9da6')
    github_secret = T.str.default('e98cede34ab9badaaab0d30f07c8d989fa11e0ec')
    # sentry
    sentry_dsn = T.str.optional
    # celery sentry
    is_celery_process = T.bool.optional

    @classmethod
    def load(cls, environ=None):
        if environ is None:
            environ = os.environ
        configs = {}
        for name in fields(cls):
            key = ('RSSANT_' + name).upper()
            configs[name] = environ.get(key, None)
        return cls(configs)
