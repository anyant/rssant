from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class PostgresClient:
    def __init__(self, dsn: str, table: str):
        self.dsn = dsn
        self.table = table

    def create_engine(self) -> Engine:
        # https://docs.sqlalchemy.org/en/13/core/pooling.html
        engine: Engine = create_engine(
            self.dsn,
            pool_size=5, max_overflow=0,
            pool_pre_ping=False,
            pool_recycle=600,
        )
        return engine

    def get_table(self) -> str:
        return self.table
