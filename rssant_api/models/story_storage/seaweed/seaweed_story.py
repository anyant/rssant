from typing import List, Tuple

from ..common.story_data import StoryData
from .seaweed_sharding import seaweed_fid_encode, SeaweedFileType
from .seaweed_client import SeaweedClient


_KEY = Tuple[int, int]


class SeaweedStoryStorage:
    def __init__(self, client: SeaweedClient):
        self._client: SeaweedClient = client

    def _content_fid(self, feed_id: int, offset: int) -> str:
        return seaweed_fid_encode(feed_id, offset, SeaweedFileType.CONTENT)

    def get_content(self, feed_id: int, offset: int) -> str:
        content_data = self._client.get(self._content_fid(feed_id, offset))
        if content_data:
            content = StoryData.decode_text(content_data)
        else:
            content = None
        return content

    def delete_content(self, feed_id: int, offset: int) -> None:
        self._client.delete(self._content_fid(feed_id, offset))

    def save_content(self, feed_id: int, offset: int, content: str) -> None:
        if not content:
            return self.delete_content(feed_id, offset)
        content_data = StoryData.encode_text(content)
        self._client.put(self._content_fid(feed_id, offset), content_data)

    def batch_get_content(self, keys: List[_KEY]) -> List[Tuple[_KEY, str]]:
        fid_s = {}
        for feed_id, offset in keys:
            fid = self._content_fid(feed_id, offset)
            fid_s[fid] = (feed_id, offset)
        result = []
        for fid, content in self._client.batch_get(fid_s.keys()).items():
            feed_id, offset = fid_s[fid]
            result.append(((feed_id, offset), content))
        return result

    def batch_delete_content(self, keys: List[_KEY]) -> None:
        for feed_id, offset in keys:
            self.delete_content(feed_id, offset)

    def batch_save_content(self, items: List[Tuple[_KEY, str]]) -> None:
        for (feed_id, offset), content in items:
            self.save_content(feed_id, offset, content)
