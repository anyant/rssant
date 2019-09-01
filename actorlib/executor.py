import logging
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

import asyncio
import aiojobs
from attrdict import AttrDict

from .message import ActorMessage
from .helper import unsafe_kill_thread
from .registery import ActorRegistery
from .sender import MessageSender
from .client import AsyncActorClient, ActorClient
from .storage import ActorStorageBase
from .message_queue import ActorMessageQueue
from .context import StorageHelper, ActorContext


LOG = logging.getLogger(__name__)


def normalize_concurrency(concurrency):
    if concurrency <= 3:
        num_async_workers = 1
    elif concurrency <= 10:
        num_async_workers = 2
    else:
        num_async_workers = 3
    num_threads = max(1, concurrency - num_async_workers)
    num_pool_workers = max(1, num_threads // 3)
    num_thread_workers = max(1, num_threads - num_pool_workers)
    concurrency = num_async_workers + num_pool_workers + num_thread_workers
    return AttrDict(
        concurrency=concurrency,
        num_async_workers=num_async_workers,
        num_pool_workers=num_pool_workers,
        num_thread_workers=num_thread_workers,
    )


class ActorExecutor:
    def __init__(
        self,
        actors,
        sender: MessageSender,
        storage: ActorStorageBase,
        registery: ActorRegistery,
        concurrency=100,
        token=None,
    ):
        self.actors = actors
        self.sender = sender
        self.storage = storage
        self.registery = registery
        self.token = token
        self.storage_helper = StorageHelper(storage, registery)
        concurrency_info = normalize_concurrency(concurrency)
        self.concurrency = concurrency_info.concurrency
        self.num_async_workers = concurrency_info.num_async_workers
        self.num_pool_workers = concurrency_info.num_pool_workers
        self.num_thread_workers = concurrency_info.num_thread_workers
        self.thread_inbox = ActorMessageQueue(self.concurrency * 10)
        self.async_inbox = ActorMessageQueue(self.concurrency * 10)
        self.threads = []
        # main objects used in receiver(http server) threads or eventloop
        self.main_event_loop = asyncio.get_event_loop()
        self.main_async_client = AsyncActorClient(registery=self.registery, token=self.token)
        self.main_thread_client = ActorClient(registery=self.registery, token=self.token)
        self.thread_pool = ThreadPoolExecutor(
            self.num_pool_workers, thread_name_prefix='actor_pool_worker_')

    async def async_handle_ask(self, message: ActorMessage) -> object:
        """Handle ask message from receiver and local async actor"""
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        LOG.info(f'handle ask message {message}')
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message, executor=self,
            actor_client=self.main_async_client)
        if actor.is_async:
            return await ctx._async_execute()
        else:
            return await self.main_event_loop.run_in_executor(
                self.thread_pool, ctx._thread_execute)

    def handle_ask(self, message: ActorMessage) -> object:
        """Handle ask message from local thread actor (or thread receiver)"""
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        LOG.info(f'handle ask message {message}')
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message, executor=self,
            actor_client=self.main_thread_client)
        if actor.is_async:
            fut = asyncio.run_coroutine_threadsafe(
                ctx._async_execute, self.main_event_loop)
            return fut.result()
        else:
            return ctx._thread_execute()

    async def async_submit(self, message: ActorMessage) -> None:
        """Handle tell or hope message from receiver and local async actor"""
        if self._is_ack_message(message):
            LOG.info(f'handle ack message {message}')
            return await self._async_handle_ack(message)
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        LOG.info(f'submit message {message}')
        actor = self.actors[message.dst]
        if actor.is_async:
            await self.async_inbox.async_put(message)
        else:
            await self.thread_inbox.async_put(message)

    def submit(self, message: ActorMessage) -> None:
        """Handle tell or hope message from local actor (or thread receiver)"""
        if self._is_ack_message(message):
            LOG.info(f'handle ack message {message}')
            return self._thread_handle_ack(message)
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        LOG.info(f'submit message {message}')
        actor = self.actors[message.dst]
        if actor.is_async:
            self.async_inbox.put(message)
        else:
            self.thread_inbox.put(message)

    def thread_main(self):
        actor_client = ActorClient(registery=self.registery, token=self.token)
        with actor_client:
            while True:
                try:
                    message = self.thread_inbox.get()
                except Exception as ex:
                    LOG.exception(ex)
                else:
                    self._handle_message(message, actor_client=actor_client)

    async def _async_main(self):
        scheduler = await aiojobs.create_scheduler(
            limit=self.concurrency, pending_limit=self.concurrency)
        actor_client = AsyncActorClient(registery=self.registery, token=self.token)
        async with actor_client:
            try:
                while True:
                    try:
                        message = await self.async_inbox.async_get()
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

    def _is_ack_message(self, message: ActorMessage):
        is_local = self.registery.is_local_message(message)
        return (not message.is_ask) and is_local and message.dst == 'actor.ack'

    def _is_deliverable(self, message: ActorMessage):
        is_local = self.registery.is_local_message(message)
        return is_local and message.dst in self.actors

    async def _async_handle_ack(self, message: ActorMessage) -> None:
        """Handle ack message from async receiver"""
        ack_msg = self.storage_helper.handle_ack_message(message)
        if ack_msg:
            await self.sender.async_submit(ack_msg)

    def _thread_handle_ack(self, message: ActorMessage) -> None:
        """Handle ack message from thread receiver"""
        ack_msg = self.storage_helper.handle_ack_message(message)
        if ack_msg:
            self.sender.submit(ack_msg)

    def _handle_message(self, message: ActorMessage, actor_client):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message, executor=self,
            actor_client=actor_client)
        ctx._thread_execute()

    async def _async_handle_message(self, message: ActorMessage, actor_client):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message, executor=self,
            actor_client=actor_client)
        await ctx._async_execute()

    def start(self):
        for i in range(self.num_async_workers):
            t = Thread(target=self.async_main, name=f'actor_async_worker_{i}')
            self.threads.append(t)
        for i in range(self.num_thread_workers):
            t = Thread(target=self.thread_main, name=f'actor_thread_worker_{i}')
            self.threads.append(t)
        for t in self.threads:
            t.daemon = True
            t.start()

    def shutdown(self):
        self.thread_pool.shutdown(wait=False)
        for t in self.threads:
            if t.is_alive():
                unsafe_kill_thread(t.ident)
        self.main_thread_client.close()
        # TODO: close self.main_async_client

    def join(self):
        for t in self.threads:
            t.join()
        self.thread_pool.join()
