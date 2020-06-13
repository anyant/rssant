from typing import List, Tuple

from sqlalchemy.sql import text as sql

from ..common.story_key import StoryId
from ..common.story_data import StoryData
from .postgres_client import PostgresClient


_KEY = Tuple[int, int]


class PostgresStoryStorage:

    def __init__(self, client: PostgresClient):
        self._engine = client.create_engine()
        self._table = client.get_table()

    def get_content(self, feed_id: int, offset: int) -> str:
        r = self.batch_get_content([(feed_id, offset)])
        if not r:
            return None
        _, content = r[0]
        return content

    def delete_content(self, feed_id: int, offset: int) -> None:
        self.batch_delete_content([(feed_id, offset)])

    def save_content(self, feed_id: int, offset: int, content: str) -> None:
        self.batch_save_content([((feed_id, offset), content)])

    @staticmethod
    def _to_id_list(keys: List[_KEY]):
        return tuple(StoryId.encode(feed_id, offset) for feed_id, offset in keys)

    def batch_get_content(self, keys: List[_KEY]) -> List[Tuple[_KEY, str]]:
        result = []
        if not keys:
            return result
        q = sql("""
        SELECT id, content FROM {table} WHERE id IN :id_list
        """.format(table=self._table))
        id_list = self._to_id_list(keys)
        with self._engine.connect() as conn:
            rows = list(conn.execute(q, id_list=id_list).fetchall())
        for story_id, content_data in rows:
            key = StoryId.decode(story_id)
            if content_data:
                content = StoryData.decode_text(content_data)
            else:
                content = None
            result.append((key, content))
        return result

    def batch_delete_content(self, keys: List[_KEY]) -> None:
        if not keys:
            return
        q = sql("""
        DELETE FROM {table} WHERE id IN :id_list
        """.format(table=self._table))
        id_list = self._to_id_list(keys)
        with self._engine.connect() as conn:
            with conn.begin():
                conn.execute(q, id_list=id_list)

    def batch_save_content(self, items: List[Tuple[_KEY, str]]) -> None:
        if not items:
            return
        q = sql("""
        INSERT INTO {table} (id, content) VALUES (:id, :content)
        ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content
        """.format(table=self._table))
        params = []
        for (feed_id, offset), content in items:
            story_id = StoryId.encode(feed_id, offset)
            if content:
                content_data = StoryData.encode_text(content)
            else:
                content_data = b''
            params.append({'id': story_id, 'content': content_data})
        with self._engine.connect() as conn:
            with conn.begin():
                conn.execute(q, params)
