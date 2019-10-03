import json
import aioredis

from rssant_config import CONFIG


KEY_PREFIX = "rssant_async_api_"
STORY_KEY_PREFIX = KEY_PREFIX + 'story_'
STORY_KEY_EXPIRE = 30 * 60


class RedisDao:
    def __init__(self):
        self.pool = None

    async def init(self):
        if self.pool is None:
            self.pool = await aioredis.create_redis_pool(
                CONFIG.redis_url, minsize=5, maxsize=20)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def set_story(self, id, story):
        key = f'{STORY_KEY_PREFIX}{id}'
        value = json.dumps(story)
        await self.pool.set(key, value)
        await self.pool.expire(key, STORY_KEY_EXPIRE)

    async def get_story(self, id):
        key = f'{STORY_KEY_PREFIX}{id}'
        value = await self.pool.get(key)
        if not value:
            return None
        return json.loads(value)


REDIS_DAO = RedisDao()
