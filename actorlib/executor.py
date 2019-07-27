import typing
import queue
import logging
import functools
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


LOG = logging.getLogger(__name__)


class ActorExecutor:
    def __init__(self, actors, sender: MessageSender, registery: ActorRegistery, concurrency=100):
        self.actors = actors
        self.sender = sender
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

    async def _async_handle_message(self, message, state, actor_client):
        try:
            actor = self.actors[message.dst]
            ctx = ActorContext(executor=self, actor=actor, state=state,
                               message=message, actor_client=actor_client)
            return await actor(ctx)
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
                            message, state={}, actor_client=actor_client)
                        await scheduler.spawn(task)
            finally:
                await scheduler.close()

    def async_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._async_main())

    def _handle_message(self, message, state, actor_client):
        try:
            actor = self.actors[message.dst]
            ctx = ActorContext(executor=self, actor=actor, state=state,
                               message=message, actor_client=actor_client)
            return actor(ctx)
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
                    self._handle_message(message, state={}, actor_client=actor_client)

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
            kwargs = dict(message=message, state={}, actor_client=self.main_async_client)
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
            kwargs = dict(message=message, state={}, actor_client=self.main_thread_client)
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
    def __init__(self, executor: ActorExecutor, actor: Actor, state: dict,
                 message: ActorMessage, actor_client: typing.Union[AsyncActorClient, ActorClient]):
        self.executor = executor
        self.registery = executor.registery
        self.actor = actor
        self.state = state
        self.message = message
        self.actor_client = actor_client

    def tell(self, dst, content=None, dst_node=None):
        if content is None:
            content = {}
        msg = ActorMessage(
            content=content, src=self.actor.name,
            dst=dst, dst_node=dst_node,
        )
        if self.actor.is_async:
            return self.executor.async_submit(msg)
        else:
            return self.executor.submit(msg)

    def ask(self, dst, content=None):
        if content is None:
            content = {}
        return self.actor_client.ask(dst, content)
