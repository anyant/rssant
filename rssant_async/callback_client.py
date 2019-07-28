import logging

import aiohttp
from rssant_common.helper import pretty_format_json, aiohttp_raise_for_status


LOG = logging.getLogger(__name__)


class CallbackClient:
    def __init__(self):
        self.session = None
        self.request_timeout = 30

    @classmethod
    def _get_instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()
        return cls._instance

    async def _async_init(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout),
            )

    async def _send(self, callback_url, data):
        if not callback_url:
            LOG.info('callback url not provide, data:\n' + pretty_format_json(data))
            return
        await self._async_init()
        async with self.session.post(callback_url, json=data) as r:
            return await r.json()
        aiohttp_raise_for_status(r)

    async def _close(self):
        if self.session:
            await self.session.close()

    @classmethod
    async def close(cls, *args, **kwargs):
        client = cls._get_instance()
        await client._close()

    @classmethod
    async def send(cls, callback_url, data):
        client = cls._get_instance()
        try:
            await client._send(callback_url, data)
        except Exception as ex:
            LOG.info(f'send callback {callback_url} failed: {ex}')
            raise
