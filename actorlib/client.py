import logging
from typing import List
from collections import defaultdict

import asyncio
import aiohttp
import requests

from rssant_common.helper import aiohttp_raise_for_status

from .message import ActorMessage, ContentEncoding
from .registery import ActorRegistery
from .helper import shorten
from .sentry import sentry_scope

LOG = logging.getLogger(__name__)


class ActorClientBase:

    def __init__(
        self, registery: ActorRegistery,
        content_encoding=ContentEncoding.MSGPACK_GZIP,
        timeout=30,
    ):
        self.registery = registery
        self.content_encoding = ContentEncoding.of(content_encoding)
        self.timeout = timeout
        self.session = None
        self.headers = {'Actor-Content-Encoding': self.content_encoding.value}
        if self.content_encoding == ContentEncoding.JSON:
            self.headers['Content-Type'] = 'application/json; charset=utf-8'

    def _group_messages(self, messages):
        groups = defaultdict(lambda: [])
        for msg in messages:
            msg = self.registery.complete_message(msg)
            if not msg.dst_url:
                LOG.error(f'no dst url for message {msg}')
                continue
            groups[msg.dst_url].append(msg)
        return groups

    def _get_ask_request(self, dst, content, dst_node=None):
        with sentry_scope() as scope:
            if dst_node is None:
                dst_node = self.registery.choice_dst_node(dst)
            scope.set_tag('ask_dst_node', dst_node)
            dst_url = self.registery.choice_dst_url(dst_node)
            scope.set_tag('ask_dst_url', dst_url)
            short_content = shorten(repr(content), width=30)
            if not dst_url:
                raise ValueError(
                    f'no dst url for ask {dst} dst_node={dst_node} content={short_content}')
            LOG.info(f'ask {dst} at {dst_node} {dst_url}: {short_content}')
            data = ActorMessage.raw_encode(content, self.content_encoding)
            headers = self.headers.copy()
            headers['Actor-DST'] = dst
            return dst_url, headers, data

    def _decode_ask_response(self, content, headers):
        if not content:
            raise ValueError('not receive reply')
        content_encoding = headers.get('Actor-Content-Encoding')
        content_encoding = ContentEncoding.of(content_encoding)
        result = ActorMessage.raw_decode(content, content_encoding)
        return result


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
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('message_dst_url', dst_url)
            LOG.info(f'send {len(messages)} messages to {dst_url}')
            data = ActorMessage.batch_encode(messages, self.content_encoding)
            try:
                r = self.session.post(
                    dst_url, data=data, headers=self.headers, timeout=self.timeout)
            except requests.ConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                return
            r.raise_for_status()

    def _batch_send(self, messages: List[ActorMessage]):
        self._init()
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            self._group_send(dst_url, items)

    def send(self, *messages):
        self._batch_send(messages)

    def ask(self, dst, content, dst_node=None):
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('ask_dst', dst)
            scope.set_tag('ask_dst_node', dst_node)
            self._init()
            dst_url, headers, data = self._get_ask_request(dst, content, dst_node=dst_node)
            try:
                r = self.session.post(dst_url, data=data, headers=headers, timeout=self.timeout)
            except requests.ConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                raise
            r.raise_for_status()
            return self._decode_ask_response(r.content, r.headers)


class AsyncActorClient(ActorClientBase):

    async def _async_init(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
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
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('message_dst_url', dst_url)
            LOG.info(f'send {len(messages)} messages to {dst_url}')
            data = ActorMessage.batch_encode(messages, self.content_encoding)
            try:
                async with self.session.post(dst_url, data=data, headers=self.headers) as r:
                    await r.read()
            except aiohttp.ClientConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                return
            aiohttp_raise_for_status(r)

    async def _batch_send(self, messages: List[ActorMessage]):
        await self._async_init()
        tasks = []
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            tasks.append(self._group_send(dst_url, items))
        await asyncio.gather(*tasks)

    async def send(self, *messages):
        await self._batch_send(messages)

    async def ask(self, dst, content, dst_node=None):
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('ask_dst', dst)
            scope.set_tag('ask_dst_node', dst_node)
            await self._async_init()
            dst_url, headers, data = self._get_ask_request(dst, content, dst_node=dst_node)
            try:
                async with self.session.post(dst_url, data=data, headers=headers) as r:
                    headers = r.headers
                    content = await r.read()
            except aiohttp.ClientConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                raise
            aiohttp_raise_for_status(r)
            return self._decode_ask_response(content, headers)
