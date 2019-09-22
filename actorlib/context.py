import typing
import logging
import contextlib
import asyncio
from concurrent.futures import Future as ThreadFuture

from .sentry import sentry_scope
from .actor import Actor
from .message import ActorMessage
from .client import AsyncActorClient, ActorClient
from .registery import ActorRegistery
from .queue import ActorMessageQueue
from .state import ERROR, OK


LOG = logging.getLogger(__name__)


class ActorContext:
    def __init__(
        self, *,
        actor: Actor,
        message: ActorMessage,
        registery: ActorRegistery,
        queue: ActorMessageQueue,
        actor_client: typing.Union[AsyncActorClient, ActorClient]
    ):
        self.actor = actor
        self.message = message
        self.registery = registery
        self._queue = queue
        self._actor_client = actor_client
        self._outbox_messages = []

    def _thread_execute(self):
        """Execute actor in thread worker"""
        try:
            with self._set_sentry_scope(self.message):
                if not self._preprocess():
                    return
                ret = None
                try:
                    ret = self.actor(self)
                except Exception as ex:
                    self._postprocess(None, ex)
                else:
                    self._postprocess(ret, None)
                return ret
        finally:
            self._close()

    async def _async_execute(self):
        """Execute actor in async worker"""
        try:
            with self._set_sentry_scope(self.message):
                if not self._preprocess():
                    return
                ret = None
                try:
                    ret = await self.actor(self)
                except Exception as ex:
                    self._postprocess(None, ex)
                else:
                    self._postprocess(ret, None)
                return ret
        finally:
            self._close()

    @contextlib.contextmanager
    def _set_sentry_scope(self, message):
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('message_src', message.src)
            scope.set_tag('message_src_node', message.src_node)
            scope.set_tag('message_dst', message.dst)
            scope.set_tag('message_dst_node', message.dst_node)
            yield message

    def _close(self):
        self._executor = None
        self._storage_helper = None
        self._storage = None
        self._sender = None
        self._actor_client = None
        self._outbox_messages = None

    def _preprocess(self) -> bool:
        """return can process or not"""
        return True

    def _postprocess(self, result, error):
        """return ack message if need ack"""
        if error:
            LOG.exception(f'actor {self.message.dst} handle {self.message} failed: {error}')
            self._queue.op_done(message_id=self.message.id, status=ERROR)
            if self.message.future:
                self.message.future.set_exception(error)
        else:
            if not self._outbox_messages:
                self._queue.op_done(message_id=self.message.id, status=OK)
            else:
                self._queue.op_outbox(message_id=self.message.id,
                                      outbox_messages=self._outbox_messages)
            if self.message.future:
                self.message.future.set_result(result)

    def _append_message(self, dst, content=None, dst_node=None, priority=None, require_ack=False, expire_at=None):
        msg = self.registery.create_message(
            content=content,
            src=self.actor.name,
            dst=dst,
            dst_node=dst_node,
            priority=priority,
            require_ack=require_ack,
            expire_at=expire_at,
            parent_id=self.message.id,
        )
        self._outbox_messages.append(msg)
        return msg

    async def _awaitable_none(self):
        return None

    def tell(self, dst, content=None, dst_node=None, priority=None, expire_at=None):
        """Require ack, will retry if failed"""
        self._append_message(
            dst=dst,
            content=content,
            dst_node=dst_node,
            require_ack=True,
            priority=priority,
            expire_at=expire_at,
        )
        if self.actor.is_async:
            return self._awaitable_none()

    def hope(self, dst, content=None, dst_node=None, priority=None, expire_at=None):
        """Fire and fogot, not require ack"""
        self._append_message(
            dst=dst,
            content=content,
            dst_node=dst_node,
            priority=priority,
            expire_at=expire_at,
        )
        if self.actor.is_async:
            return self._awaitable_none()

    def ask(self, dst, content=None, dst_node=None):
        """Send request and wait response"""
        if not dst_node:
            dst_node = self.registery.choice_dst_node(dst)
        msg = self.registery.create_message(
            dst=dst,
            is_ask=True,
            content=content,
            src=self.actor.name,
            dst_node=dst_node,
        )
        if msg.is_local:
            future = ThreadFuture()
            msg.future = future
            if self.actor.is_async:
                return asyncio.wrap_future(future)
            else:
                return future.result()
        else:
            return self._actor_client.ask(msg)
