from typing import List, Tuple, Dict
from collections import defaultdict

from sqlalchemy.sql import text as sql

from ..common.story_key import StoryId
from ..common.story_data import StoryData
from .postgres_client import PostgresClient
from .postgres_sharding import sharding_for


_KEY = Tuple[int, int]


class PostgresStoryStorage:

    def __init__(self, client: PostgresClient):
        self._client = client

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

    @classmethod
    def _split_by(cls, items: list, by: callable) -> dict:
        groups = defaultdict(list)
        for item in items:
            groups[by(item)].append(item)
        return groups

    @classmethod
    def _split_keys(cls, keys: List[_KEY]) -> Dict[int, List[_KEY]]:
        return cls._split_by(keys, lambda x: sharding_for(x[0]))

    @classmethod
    def _split_items(cls, items: List[Tuple[_KEY, str]]) -> Dict[int, List[Tuple[_KEY, str]]]:
        return cls._split_by(items, lambda x: sharding_for(x[0][0]))

    @staticmethod
    def _to_id_tuple(keys: List[_KEY]) -> tuple:
        return tuple(StoryId.encode(feed_id, offset) for feed_id, offset in keys)

    def batch_get_content(self, keys: List[_KEY]) -> List[Tuple[_KEY, str]]:
        result = []
        if not keys:
            return result
        groups = self._split_keys(keys)
        for volume, group_keys in groups.items():
            result.extend(self._batch_get_content(volume, group_keys))
        return result

    def _batch_get_content(self, volume: int, keys: List[_KEY]) -> List[Tuple[_KEY, str]]:
        q = sql("""
        SELECT id, content FROM {table} WHERE id IN :id_tuple
        """.format(table=self._client.get_table(volume)))
        id_tuple = self._to_id_tuple(keys)
        with self._client.get_engine(volume).connect() as conn:
            rows = list(conn.execute(q, id_tuple=id_tuple).fetchall())
        result = []
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
        groups = self._split_keys(keys)
        for volume, group_keys in groups.items():
            self._batch_delete_content(volume, group_keys)

    def _batch_delete_content(self, volume: int, keys: List[_KEY]) -> None:
        q = sql("""
        DELETE FROM {table} WHERE id IN :id_tuple
        """.format(table=self._client.get_table(volume)))
        id_tuple = self._to_id_tuple(keys)
        with self._client.get_engine(volume).connect() as conn:
            with conn.begin():
                conn.execute(q, id_tuple=id_tuple)

    def batch_save_content(self, items: List[Tuple[_KEY, str]]) -> None:
        if not items:
            return
        groups = self._split_items(items)
        for volume, group_items in groups.items():
            self._batch_save_content(volume, group_items)

    def _batch_save_content(self, volume: int, items: List[Tuple[_KEY, str]]) -> None:
        q = sql("""
        INSERT INTO {table} (id, content) VALUES (:id, :content)
        ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content
        """.format(table=self._client.get_table(volume)))
        params = []
        for (feed_id, offset), content in items:
            story_id = StoryId.encode(feed_id, offset)
            if content:
                content_data = StoryData.encode_text(content)
            else:
                content_data = b''
            params.append({'id': story_id, 'content': content_data})
        with self._client.get_engine(volume).connect() as conn:
            with conn.begin():
                conn.execute(q, params)
