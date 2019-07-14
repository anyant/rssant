import logging
from typing import List
from collections import defaultdict

import asyncio
import aiohttp
import requests

from .message import ActorMessage, ContentEncoding
from .registery import ActorRegistery


LOG = logging.getLogger(__name__)


class ActorClientBase:

    def __init__(
        self, registery: ActorRegistery,
        content_encoding=ContentEncoding.MSGPACK_GZIP,
        timeout=30,
    ):
        self.registery = registery
        self.content_encoding = content_encoding
        self.timeout = timeout
        self.session = None

    def _group_messages(self, messages):
        groups = defaultdict(lambda: [])
        for msg in messages:
            msg = self.registery.complete_message(msg)
            groups[msg.dst_url].append(msg)
        return groups


class ActorClient(ActorClientBase):

    def _init(self):
        if self.session is None:
            self.session = requests.Session()

    def close(self):
        if self.session:
            self.session.close()
            self.session = None

    def __enter__(self):
        self._init()
        return self

    def __exit__(self, *exc):
        self.close()

    def _group_send(self, dst_url, messages):
        LOG.info(f'send {len(messages)} messages to {dst_url}')
        data = ActorMessage.batch_encode(messages, self.content_encoding)
        headers = {'Actor-Content-Encoding': self.content_encoding.value}
        r = self.session.post(dst_url, data=data, headers=headers, timeout=self.timeout)
        r.raise_for_status()

    def _batch_send(self, messages: List[ActorMessage]):
        self._init()
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            self._group_send(dst_url, items)

    def send(self, *messages):
        self._batch_send(messages)


class AsyncActorClient(ActorClientBase):

    async def _async_init(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                raise_for_status=True,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )

    async def close(self):
        if self.session:
            await self.session.close()

    async def __aenter__(self):
        await self._async_init()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _group_send(self, dst_url, messages):
        LOG.info(f'send {len(messages)} messages to {dst_url}')
        data = ActorMessage.batch_encode(messages, self.content_encoding)
        headers = {'Actor-Content-Encoding': self.content_encoding.value}
        async with self.session.post(dst_url, data=data, headers=headers) as r:
            await r.read()

    async def _batch_send(self, messages: List[ActorMessage]):
        await self._async_init()
        tasks = []
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            tasks.append(self._group_send(dst_url, items))
        await asyncio.gather(*tasks)

    async def send(self, *messages):
        await self._batch_send(messages)
