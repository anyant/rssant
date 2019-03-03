import os
from validr import T, modelclass, fields


@modelclass
class EnvConfig:

    debug = T.bool.default(True).desc('debug')
    pg_host = T.str.default('127.0.0.1').desc('postgres host')
    pg_port = T.int.default(5432).desc('postgres port')
    pg_db = T.str.default('rssant').desc('postgres database')
    pg_user = T.str.default('rssant').desc('postgres user')
    pg_password = T.str.default('rssant').desc('postgres password')
    redis_url = T.str.default('redis://127.0.0.1:6379/0').desc('redis url')

    @classmethod
    def load(cls, environ=None):
        if environ is None:
            environ = os.environ
        configs = {}
        for name in fields(cls):
            key = ('RSSANT_' + name).upper()
            configs[name] = environ.get(key, None)
        return cls(configs)
