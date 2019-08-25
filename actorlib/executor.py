import typing
import queue
import logging
import functools
import contextlib
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

import asyncio
import aiojobs

from .actor import Actor
from .message import ActorMessage
from .helper import unsafe_kill_thread
from .registery import ActorRegistery
from .sender import MessageSender
from .client import AsyncActorClient, ActorClient
from .sentry import sentry_scope
from .storage import ActorStorageBase
from .state import ActorStateError


LOG = logging.getLogger(__name__)


class ActorExecutor:
    def __init__(
        self,
        actors,
        sender: MessageSender,
        storage: ActorStorageBase,
        registery: ActorRegistery,
        concurrency=100,
    ):
        self.actors = actors
        self.sender = sender
        self.storage = storage
        self.registery = registery
        if concurrency <= 3:
            num_async_workers = 1
        elif concurrency <= 10:
            num_async_workers = 2
        else:
            num_async_workers = 3
        num_thread_workers = max(1, concurrency - num_async_workers)
        self.num_async_workers = num_async_workers
        self.num_thread_workers = num_thread_workers
        self.concurrency = num_async_workers + num_thread_workers
        self.thread_inbox = queue.Queue(self.concurrency)
        self.async_inbox = queue.Queue(self.concurrency)
        self.threads = []
        # main objects used in receiver(http server) threads or eventloop
        self.main_event_loop = asyncio.get_event_loop()
        self.main_async_client = AsyncActorClient(registery=self.registery)
        self.main_thread_client = ActorClient(registery=self.registery)
        self.thread_pool = ThreadPoolExecutor(num_thread_workers)

    async def _async_get_message(self, box):
        while True:
            try:
                return box.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def _async_put_message(self, box, message):
        while True:
            try:
                return box.put_nowait(message)
            except queue.Full:
                await asyncio.sleep(0.1)

    @contextlib.contextmanager
    def _set_sentry_scope(self, message):
        with sentry_scope() as scope:
            scope.set_tag('actor_node', self.registery.current_node_name)
            scope.set_tag('message_src', message.src)
            scope.set_tag('message_src_node', message.src_node)
            scope.set_tag('message_dst', message.dst)
            scope.set_tag('message_dst_node', message.dst_node)
            yield message

    def _handle_message_execute_begin(self, message):
        try:
            self.storage.op_begin(
                message.id,
                is_ask=message.is_ask,
                dst=message.dst,
                src=message.src,
                src_node=message.src_node,
            )
        except ActorStateError as ex:
            LOG.warning(ex)
            return True
        return False

    async def _async_handle_message(self, message, actor_client):
        with self._set_sentry_scope(message):
            if self._handle_message_execute_begin(message):
                return None
            try:
                actor = self.actors[message.dst]
                ctx = ActorContext(
                    executor=self, actor=actor,
                    message=message, actor_client=actor_client)
                ret = await actor(ctx)
                await self._async_send_ack_if_done(
                    self._handle_message_execute_done(message, ctx)
                )
                return ret
            except Exception as ex:
                LOG.exception(f'actor {message.dst} handle {message} failed: {ex}')
                try:
                    done_msg = self.storage.op_done(message.id, 'ERROR')
                except ActorStateError as ex:
                    LOG.warning(ex)
                else:
                    await self._async_send_ack_if_done(done_msg)
                return None

    async def _async_main(self):
        scheduler = await aiojobs.create_scheduler(
            limit=self.concurrency, pending_limit=self.concurrency)
        actor_client = AsyncActorClient(registery=self.registery)
        async with actor_client:
            try:
                while True:
                    try:
                        message = await self._async_get_message(self.async_inbox)
                    except Exception as ex:
                        LOG.exception(ex)
                    else:
                        task = self._async_handle_message(
                            message, actor_client=actor_client)
                        await scheduler.spawn(task)
            finally:
                await scheduler.close()

    def async_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._async_main())

    def _handle_message_execute_done(self, message, ctx):
        send_messages = [x.to_dict() for x in ctx._pop_send_messages()]
        try:
            if send_messages:
                return self.storage.op_send(message.id, send_messages)
            else:
                return self.storage.op_done(message.id, 'OK')
        except ActorStateError as ex:
            LOG.warning(ex)
            return None

    def _send_ack_if_done(self, message):
        if not message:
            return
        if message['is_ask']:
            return
        dst_node = message['src_node']
        msg_id = message['id']
        status = message['status']
        if self.registery.is_local_node(dst_node):
            try:
                done_msg = self.storage.op_ack(msg_id, status)
            except ActorStateError as ex:
                LOG.warning(ex)
            else:
                self._send_ack_if_done(done_msg)
        else:
            self.sender.submit(ActorMessage(
                id=msg_id,
                src='actor.ack',
                dst='actor.ack',
                dst_node=dst_node,
                content=dict(status=status)
            ))

    async def _async_send_ack_if_done(self, message):
        if not message:
            return
        if message['is_ask']:
            return
        dst_node = message['src_node']
        msg_id = message['id']
        status = message['status']
        if self.registery.is_local_node(dst_node):
            try:
                done_msg = self.storage.op_ack(msg_id, status)
            except ActorStateError as ex:
                LOG.warning(ex)
            else:
                await self._async_send_ack_if_done(done_msg)
        else:
            await self.sender.async_submit(ActorMessage(
                id=msg_id,
                src='actor.ack',
                dst='actor.ack',
                dst_node=dst_node,
                content=dict(status=status)
            ))

    def _handle_message(self, message, actor_client):
        with self._set_sentry_scope(message):
            if self._handle_message_execute_begin(message):
                return None
            try:
                actor = self.actors[message.dst]
                ctx = ActorContext(
                    executor=self, actor=actor,
                    message=message, actor_client=actor_client)
                ret = actor(ctx)
                self._send_ack_if_done(
                    self._handle_message_execute_done(message, ctx)
                )
                return ret
            except Exception as ex:
                LOG.exception(f'actor {message.dst} handle {message} failed: {ex}')
                try:
                    done_msg = self.storage.op_done(message.id, 'ERROR')
                except ActorStateError as ex:
                    LOG.warning(ex)
                else:
                    self._send_ack_if_done(done_msg)
                return None

    def thread_main(self):
        actor_client = ActorClient(registery=self.registery)
        with actor_client:
            while True:
                try:
                    message = self.thread_inbox.get()
                except Exception as ex:
                    LOG.exception(ex)
                else:
                    self._handle_message(message, actor_client=actor_client)

    def _check_process_ack_message(self, message):
        is_local = self.registery.is_local_message(message)
        if (not message.is_ask) and is_local and message.dst == 'actor.ack':
            try:
                return True, self.storage.op_ack(message.id, message.content['status'])
            except ActorStateError as ex:
                LOG.warning(ex)
            return True, None
        return False, None

    def _check_message_undeliverable(self, message):
        is_local = self.registery.is_local_message(message)
        if not (is_local and message.dst in self.actors):
            LOG.error(f'undeliverable message {message}')
            return True
        return False

    async def async_submit(self, message):
        message = self.registery.complete_message(message)
        LOG.info(f'submit message {message}')
        if not self.registery.is_local_message(message):
            await self.sender.async_submit(message)
        else:
            await self.async_on_message(message)

    async def async_on_message(self, message):
        is_ack_message, done_msg = self._check_process_ack_message(message)
        if is_ack_message:
            await self._async_send_ack_if_done(done_msg)
            return
        if self._check_message_undeliverable(message):
            return
        actor = self.actors[message.dst]
        if message.is_ask:
            kwargs = dict(message=message, actor_client=self.main_async_client)
            if actor.is_async:
                return await self._async_handle_message(**kwargs)
            else:
                task = functools.partial(self._handle_message, **kwargs)
                return await self.main_event_loop.run_in_executor(
                    self.thread_pool, task)
        else:
            if actor.is_async:
                await self._async_put_message(self.async_inbox, message)
            else:
                await self._async_put_message(self.thread_inbox, message)

    def on_message(self, message):
        is_ack_message, done_msg = self._check_process_ack_message(message)
        if is_ack_message:
            self._send_ack_if_done(done_msg)
            return
        if self._check_message_undeliverable(message):
            return
        actor = self.actors[message.dst]
        if message.is_ask:
            kwargs = dict(message=message, actor_client=self.main_thread_client)
            if actor.is_async:
                fut = asyncio.run_coroutine_threadsafe(
                    self._async_handle_message(**kwargs), self.main_event_loop)
            else:
                fut = self.thread_pool.submit(self._handle_message, **kwargs)
            return fut.result()
        else:
            if actor.is_async:
                self.async_inbox.put(message)
            else:
                self.thread_inbox.put(message)

    def submit(self, message):
        message = self.registery.complete_message(message)
        LOG.info(f'submit message {message}')
        if not self.registery.is_local_message(message):
            self.sender.submit(message)
        else:
            self.on_message(message)

    def start(self):
        for i in range(self.num_async_workers):
            t = Thread(target=self.async_main)
            self.threads.append(t)
        for i in range(self.num_thread_workers):
            t = Thread(target=self.thread_main)
            self.threads.append(t)
        for t in self.threads:
            t.daemon = True
            t.start()

    def shutdown(self):
        for t in self.threads:
            if t.is_alive():
                unsafe_kill_thread(t.ident)
        self.main_thread_client.close()
        # TODO: close self.main_async_client

    def join(self):
        for t in self.threads:
            t.join()


class ActorContext:
    def __init__(self, executor: ActorExecutor, actor: Actor,
                 message: ActorMessage, actor_client: typing.Union[AsyncActorClient, ActorClient]):
        self.executor = executor
        self.registery = executor.registery
        self.actor = actor
        self.message = message
        self.actor_client = actor_client
        self.send_messages = []

    def _pop_send_messages(self):
        ret = self.send_messages
        self.send_messages = []
        return ret

    def _append_message(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
        )
        msg = self.registery.complete_message(msg)
        self.send_messages.append(msg)
        return msg

    def tell(self, dst, content=None, dst_node=None):
        msg = self._append_message(dst, content=content, dst_node=dst_node)
        if self.actor.is_async:
            return self.executor.async_submit(msg)
        else:
            return self.executor.submit(msg)

    def ask(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
        )
        return self.actor_client.ask(msg)
