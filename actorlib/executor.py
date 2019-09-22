import logging
from threading import Thread

import asyncio
import aiojobs
from attrdict import AttrDict

from .message import ActorMessage
from .helper import unsafe_kill_thread, auto_restart_when_crash
from .registery import ActorRegistery
from .client import AsyncActorClient, ActorClient
from .queue import ActorMessageQueue
from .context import ActorContext


LOG = logging.getLogger(__name__)


def normalize_concurrency(concurrency):
    num_async_workers = concurrency // 30 + 1
    num_thread_workers = max(1, concurrency - num_async_workers)
    concurrency = num_async_workers + num_thread_workers
    async_concurrency = concurrency * 10 / num_async_workers
    async_pending_limit = max(10, concurrency // 10)
    return AttrDict(
        concurrency=concurrency,
        num_async_workers=num_async_workers,
        num_thread_workers=num_thread_workers,
        async_concurrency=async_concurrency,
        async_pending_limit=async_pending_limit,
    )


class ActorExecutor:
    def __init__(
        self,
        actors,
        queue: ActorMessageQueue,
        registery: ActorRegistery,
        concurrency=100,
        token=None,
    ):
        self.actors = actors
        self.queue = queue
        self.registery = registery
        self.token = token
        concurrency_info = normalize_concurrency(concurrency)
        self.concurrency = concurrency_info.concurrency
        self.num_async_workers = concurrency_info.num_async_workers
        self.num_thread_workers = concurrency_info.num_thread_workers
        self.async_concurrency = concurrency_info.async_concurrency
        self.async_pending_limit = concurrency_info.async_pending_limit
        self.threads = []

    @auto_restart_when_crash
    def thread_main(self):
        actor_client = ActorClient(registery=self.registery, token=self.token)
        with actor_client:
            while True:
                try:
                    message = self.queue.op_execute()
                    self._handle_message(message, actor_client=actor_client)
                except Exception as ex:
                    LOG.exception(ex)

    @auto_restart_when_crash
    async def _async_main(self):
        scheduler = await aiojobs.create_scheduler(
            limit=self.async_concurrency, pending_limit=self.async_pending_limit)
        actor_client = AsyncActorClient(registery=self.registery, token=self.token)
        async with actor_client:
            try:
                while True:
                    try:
                        message = await self.queue.async_op_execute()
                        task = self._async_handle_message(message, actor_client=actor_client)
                        await scheduler.spawn(task)
                    except Exception as ex:
                        LOG.exception(ex)
            finally:
                await scheduler.close()

    @auto_restart_when_crash
    def async_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._async_main())

    def _is_deliverable(self, message: ActorMessage):
        return message.dst in self.actors

    def _handle_message(self, message: ActorMessage, actor_client):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message,
            registery=self.registery, queue=self.queue,
            actor_client=actor_client)
        ctx._thread_execute()

    async def _async_handle_message(self, message: ActorMessage, actor_client):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        ctx = ActorContext(
            actor=actor, message=message,
            registery=self.registery, queue=self.queue,
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
        for t in self.threads:
            if t.is_alive():
                unsafe_kill_thread(t.ident)

    def join(self):
        for t in self.threads:
            t.join()
