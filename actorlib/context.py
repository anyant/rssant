import typing
import logging
import contextlib

import actorlib.executor
from .sentry import sentry_scope
from .actor import Actor
from .message import ActorMessage
from .client import AsyncActorClient, ActorClient
from .state import ActorStateError
from .storage import ActorStorageBase
from .registery import ActorRegistery


LOG = logging.getLogger(__name__)


class StorageHelper:

    def __init__(self, storage: ActorStorageBase, registery: ActorRegistery):
        self.storage = storage
        self.registery = registery

    def execute_op(self, op, *args, **kwargs):
        try:
            done_message = op(*args, **kwargs)
        except ActorStateError as ex:
            LOG.warning(ex)
        else:
            if done_message and done_message['require_ack']:
                return self.handle_ack(
                    message_id=done_message['id'],
                    dst_node=done_message['src_node'],
                    status=done_message['status'],
                )
        return None

    def handle_ack(self, message_id, dst_node, status) -> None:
        if not self.registery.is_local_node(dst_node):
            ack_msg = ActorMessage(
                id=message_id,
                src='actor.ack',
                dst='actor.ack',
                dst_node=dst_node,
                content=dict(status=status)
            )
            return self.registery.complete_message(ack_msg)
        return self.execute_op(self.storage.op_ack, message_id, status)

    def handle_ack_message(self, message: ActorMessage):
        """Handle ack message from receiver"""
        status = message.content['status']
        return self.execute_op(self.storage.op_ack, message.id, status)


class ActorContext:
    def __init__(
        self, *,
        actor: Actor,
        message: ActorMessage,
        executor: "actorlib.executor.ActorExecutor",
        actor_client: typing.Union[AsyncActorClient, ActorClient]
    ):
        self.actor = actor
        self.message = message
        self.registery = executor.registery
        self._executor = executor
        self._storage_helper = executor.storage_helper
        self._storage = executor.storage
        self._sender = executor.sender
        self._actor_client = actor_client
        self._send_messages = []

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
                    ack_msg = self._postprocess(None, ex)
                else:
                    ack_msg = self._postprocess(ret, None)
                if ack_msg:
                    self._sender.submit(ack_msg)
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
                    ack_msg = self._postprocess(None, ex)
                else:
                    ack_msg = self._postprocess(ret, None)
                if ack_msg:
                    await self._sender.async_submit(ack_msg)
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
        self._send_messages = None

    def _preprocess(self) -> bool:
        """return can process or not"""
        try:
            self._storage.op_begin(
                self.message.id,
                require_ack=self.message.require_ack,
                dst=self.message.dst,
                src=self.message.src,
                src_node=self.message.src_node,
            )
        except ActorStateError as ex:
            LOG.warning(ex)
            return False
        return True

    def _postprocess(self, result, error):
        """return ack message if need ack"""
        if error:
            LOG.exception(f'actor {self.message.dst} handle {self.message} failed: {error}')
            ack_msg = self._storage_helper.execute_op(
                self._storage.op_done, self.message.id, 'ERROR')
        else:
            send_messages = [x.to_dict() for x in self._send_messages if x.require_ack]
            if not send_messages:
                ack_msg = self._storage_helper.execute_op(
                    self._storage.op_done, self.message.id, 'OK')
            else:
                ack_msg = self._storage_helper.execute_op(
                    self._storage.op_send, self.message.id, send_messages)
        if self.message.require_ack and ack_msg:
            return ack_msg
        return None

    def _append_message(self, dst, content=None, dst_node=None, require_ack=False):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
            is_ask=False, require_ack=require_ack,
        )
        msg = self.registery.complete_message(msg)
        self._send_messages.append(msg)
        return msg

    def _send_message(self, message):
        if self.registery.is_local_message(message):
            if self.actor.is_async:
                return self._executor.async_submit(message)
            else:
                return self._executor.submit(message)
        else:
            if self.actor.is_async:
                return self._sender.async_submit(message)
            else:
                return self._sender.submit(message)

    def tell(self, dst, content=None, dst_node=None):
        """Require ack, will retry if failed"""
        msg = self._append_message(
            dst, content=content, dst_node=dst_node, require_ack=True)
        return self._send_message(msg)

    def hope(self, dst, content=None, dst_node=None):
        """Fire and fogot, not require ack"""
        msg = self._append_message(
            dst, content=content, dst_node=dst_node, require_ack=False)
        return self._send_message(msg)

    def ask(self, dst, content=None, dst_node=None):
        """Send request and wait response"""
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            is_ask=True, require_ack=False,
            dst=dst, dst_node=dst_node,
        )
        msg = self.registery.complete_message(msg)
        if self.registery.is_local_message(msg):
            if self.actor.is_async:
                return self._executor.async_handle_ask(msg)
            else:
                return self._executor.handle_ask(msg)
        else:
            return self._actor_client.ask(msg)
