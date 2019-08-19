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
from .storage import ActorStorageBase, DuplicateMessageError


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

    async def _async_handle_message(self, message, actor_client):
        with self._set_sentry_scope(message):
            try:
                self.storage.op_begin(message.id)
            except DuplicateMessageError as ex:
                LOG.info(f'DuplicateMessageError: {ex}')
                return None
            try:
                actor = self.actors[message.dst]
                ctx = ActorContext(
                    executor=self, actor=actor,
                    message=message, actor_client=actor_client)
                try:
                    ret = await actor(ctx)
                except Exception:
                    self.storage.op_done(message.id, 'ERROR')
                    raise
                else:
                    send_messages = [x.to_dict() for x in ctx.send_messages]
                    self.storage.op_send(message.id, send_messages)
                    await ctx.flush()
                    return ret
            except Exception as ex:
                LOG.exception(f'actor {message.dst} handle {message} failed: {ex}')
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

    def _handle_message(self, message, actor_client):
        with self._set_sentry_scope(message):
            try:
                self.storage.op_begin(message.id)
            except DuplicateMessageError as ex:
                LOG.info(f'DuplicateMessageError: {ex}')
                return None
            try:
                actor = self.actors[message.dst]
                ctx = ActorContext(
                    executor=self, actor=actor,
                    message=message, actor_client=actor_client)
                try:
                    ret = actor(ctx)
                except Exception:
                    self.storage.op_done(message.id, 'ERROR')
                    raise
                else:
                    send_messages = [x.to_dict() for x in ctx.send_messages]
                    self.storage.op_send(message.id, send_messages)
                    ctx.flush()
                    return ret
            except Exception as ex:
                LOG.exception(f'actor {message.dst} handle {message} failed: {ex}')
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

    def _is_deliverable(self, message):
        return self.registery.is_local_message(message) and message.dst in self.actors

    async def async_submit(self, message):
        message = self.registery.complete_message(message)
        LOG.info(f'submit message {message}')
        if not self.registery.is_local_message(message):
            await self.sender.async_submit(message)
        else:
            await self.async_on_message(message)

    async def async_on_message(self, message, is_ask=False):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        if is_ask:
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

    def on_message(self, message, is_ask=False):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        if is_ask:
            kwargs = dict(message=message, actor_client=self.main_thread_client)
            if actor.is_async:
                fut = asyncio.run_coroutine_threadsafe(
                    self._async_handle_message(**kwargs), self.main_event_loop)
            else:
                fut = self.thread_pool.submit(self._handle_message, kwargs=kwargs)
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

    def _tell(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
        )
        self.send_messages.append(msg)

    async def _async_tell(self, dst, content=None, dst_node=None):
        self._tell(dst, content=content, dst_node=dst_node)

    def tell(self, dst, content=None, dst_node=None):
        if self.actor.is_async:
            return self._async_tell(dst, content=content, dst_node=dst_node)
        else:
            return self._tell(dst, content=content, dst_node=dst_node)

    def _flush(self):
        for msg in self.send_messages:
            self.executor.submit(msg)
        self.send_messages = []

    async def _async_flush(self):
        for msg in self.send_messages:
            await self.executor.async_submit(msg)
        self.send_messages = []

    def flush(self):
        if self.actor.is_async:
            return self._async_flush()
        else:
            return self._flush()

    def ask(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
        )
        return self.actor_client.ask(msg)
