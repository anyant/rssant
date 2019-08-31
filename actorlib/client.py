import logging
from typing import List
from collections import defaultdict
from contextlib import contextmanager

import asyncio
import aiohttp
import requests

from rssant_common.helper import aiohttp_raise_for_status, aiohttp_client_session

from .helper import hash_token
from .message import ActorMessage, ContentEncoding
from .registery import ActorRegistery
from .sentry import sentry_scope

LOG = logging.getLogger(__name__)


class ActorClientBase:

    def __init__(
        self, registery: ActorRegistery,
        content_encoding=ContentEncoding.MSGPACK_GZIP,
        timeout=30,
        token=None,
    ):
        self.registery = registery
        self.content_encoding = ContentEncoding.of(content_encoding)
        self.timeout = timeout
        self.token = hash_token(token)
        self.session = None
        self.headers = {'actor-content-encoding': self.content_encoding.value}
        if self.content_encoding == ContentEncoding.JSON:
            self.headers['content-type'] = 'application/json; charset=utf-8'
        if self.token:
            self.headers['actor-token'] = self.token

    def _group_messages(self, messages):
        groups = defaultdict(lambda: [])
        for msg in messages:
            msg = self.registery.complete_message(msg)
            if not msg.dst_url:
                LOG.error(f'no dst_url for message {msg}')
                continue
            groups[msg.dst_url].append(msg)
        return groups

    def _headers_of_ask(self, message):
        return {
            'actor-ask-id': message.id,
            'actor-ask-src': message.src,
            'actor-ask-src-node': message.src_node,
            'actor-ask-dst': message.dst,
            'actor-ask-dst-node': message.dst_node,
            'actor-ask-dst-url': message.dst_url,
        }

    def _get_ask_request(self, message):
        message = self.registery.complete_message(message)
        if not message.dst_url:
            raise ValueError(f'no dst_url for ask {message}')
        LOG.info(f'ask {message}')
        data = ActorMessage.raw_encode(message.content, self.content_encoding)
        headers = self.headers.copy()
        headers.update(self._headers_of_ask(message))
        return message, headers, data

    @contextmanager
    def _sentry_group_message_scope(self, dst_url):
        with sentry_scope() as scope:
            scope.set_tag('current_node_name', self.registery.current_node_name)
            scope.set_tag('dst_url', dst_url)
            yield scope

    @contextmanager
    def _sentry_message_scope(self, message):
        with sentry_scope() as scope:
            scope.set_tag('current_node_name', self.registery.current_node_name)
            scope.set_tag('src', message.src)
            scope.set_tag('src_node', message.src_node)
            scope.set_tag('dst', message.dst)
            scope.set_tag('dst_node', message.dst_node)
            scope.set_tag('dst_url', message.dst_url)
            yield scope

    def _decode_ask_response(self, content, headers):
        if not content:
            raise ValueError('not receive reply')
        content_encoding = headers.get('actor-content-encoding')
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
        with self._sentry_group_message_scope(dst_url):
            LOG.info(f'send {len(messages)} messages to {dst_url}')
            data = ActorMessage.batch_encode(messages, self.content_encoding)
            try:
                r = self.session.post(
                    dst_url, data=data, headers=self.headers, timeout=self.timeout)
            except requests.ConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                return
            r.raise_for_status()

    def send(self, *messages: List[ActorMessage]):
        self._init()
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            self._group_send(dst_url, items)

    def ask(self, message: ActorMessage):
        self._init()
        message, headers, data = self._get_ask_request(message)
        with self._sentry_message_scope(message):
            try:
                r = self.session.post(
                    message.dst_url, data=data,
                    headers=headers, timeout=self.timeout)
            except requests.ConnectionError as ex:
                LOG.error(f'failed to send message to {message.dst_url}: {ex}')
                raise
            r.raise_for_status()
            return self._decode_ask_response(r.content, r.headers)


class AsyncActorClient(ActorClientBase):

    async def _async_init(self):
        if self.session is None:
            self.session = aiohttp_client_session(timeout=self.timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    async def __aenter__(self):
        await self._async_init()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _group_send(self, dst_url, messages):
        with self._sentry_group_message_scope(dst_url):
            LOG.info(f'send {len(messages)} messages to {dst_url}')
            data = ActorMessage.batch_encode(messages, self.content_encoding)
            try:
                async with self.session.post(dst_url, data=data, headers=self.headers) as r:
                    await r.read()
            except aiohttp.ClientConnectionError as ex:
                LOG.error(f'failed to send message to {dst_url}: {ex}')
                return
            aiohttp_raise_for_status(r)

    async def send(self, *messages: List[ActorMessage]):
        await self._async_init()
        tasks = []
        groups = self._group_messages(messages)
        for dst_url, items in groups.items():
            tasks.append(self._group_send(dst_url, items))
        await asyncio.gather(*tasks)

    async def ask(self, message: ActorMessage):
        await self._async_init()
        message, headers, data = self._get_ask_request(message)
        with self._sentry_message_scope(message):
            try:
                async with self.session.post(message.dst_url, data=data, headers=headers) as r:
                    headers = r.headers
                    content = await r.read()
            except aiohttp.ClientConnectionError as ex:
                LOG.error(f'failed to send message to {message.dst_url}: {ex}')
                raise
            aiohttp_raise_for_status(r)
            return self._decode_ask_response(content, headers)
