import logging
from threading import RLock
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


LOG = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, volumes: dict, pool_size=5, pool_recycle=600):
        self._volumes = volumes
        self._pool_size = pool_size
        self._pool_recycle = pool_recycle
        self._dsn_s = {}
        self._table_s = {}
        for volume, cfg in volumes.items():
            dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(
                user=cfg['user'],
                password=cfg['password'],
                host=cfg['host'],
                port=cfg['port'],
                db=cfg['db'],
            )
            self._dsn_s[volume] = dsn
            self._table_s[volume] = cfg['table']
        self._engine_s = {}
        self._lock = RLock()

    def _init_table(self, engine: Engine, table: str):
        query = """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGINT PRIMARY KEY,
            content BYTEA NOT NULL
        )
        """.format(table=table)
        with engine.connect() as conn:
            conn.execute(query)

    def _create_engine(self, volume: int) -> Engine:
        LOG.info('create sqlalchemy engine for volume %s', volume)
        # https://docs.sqlalchemy.org/en/13/core/pooling.html
        engine: Engine = create_engine(
            self._dsn_s[volume],
            pool_size=self._pool_size,
            max_overflow=0,
            pool_pre_ping=False,
            pool_recycle=self._pool_recycle,
        )
        self._init_table(engine, self._table_s[volume])
        return engine

    def get_engine(self, volume: int) -> Engine:
        if volume not in self._volumes:
            raise ValueError(f'story volume {volume} not exists')
        if volume not in self._engine_s:
            with self._lock:
                if volume not in self._engine_s:
                    engine = self._create_engine(volume)
                    self._engine_s[volume] = engine
        return self._engine_s[volume]

    def get_table(self, volume: int) -> str:
        if volume not in self._volumes:
            raise ValueError(f'story volume {volume} not exists')
        return self._table_s[volume]

    def close(self):
        engine: Engine
        for engine in self._engine_s.values():
            engine.dispose()
