import queue
import logging
from threading import Thread

import asyncio
import aiojobs

from .actor import Actor
from .message import ActorMessage
from .helper import unsafe_kill_thread
from .registery import ActorRegistery
from .sender import MessageSender


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
        num_thread_workers = min(1, concurrency - num_async_workers)
        self.num_async_workers = num_async_workers
        self.num_thread_workers = num_thread_workers
        self.concurrency = num_async_workers + num_thread_workers
        self.thread_inbox = queue.Queue(self.concurrency)
        self.async_inbox = queue.Queue(self.concurrency)
        self.threads = []

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

    async def _async_main(self):
        scheduler = await aiojobs.create_scheduler(
            limit=self.concurrency, pending_limit=self.concurrency)
        try:
            state = {}
            while True:
                try:
                    message = await self._async_get_message(self.async_inbox)
                    actor = self.actors[message.dst]
                    ctx = ActorContext(
                        executor=self, actor=actor, state=state, message=message)
                    await scheduler.spawn(actor(ctx))
                except Exception as ex:
                    LOG.exception(ex)
        finally:
            await scheduler.close()

    def async_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._async_main())

    def thread_main(self):
        state = {}
        while True:
            try:
                message = self.thread_inbox.get()
                actor = self.actors[message.dst]
                ctx = ActorContext(
                    executor=self, actor=actor, state=state, message=message)
                actor(ctx)
            except Exception as ex:
                LOG.exception(ex)

    def _is_deliverable(self, message):
        return self.registery.is_local_message(message) and message.dst in self.actors

    async def async_submit(self, message):
        message = self.registery.complete_message(message)
        LOG.info(f'submit message {message}')
        if not self.registery.is_local_message(message):
            await self.sender.async_submit(message)
        else:
            await self.async_on_message(message)

    async def async_on_message(self, message):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
        if actor.is_async:
            await self._async_put_message(self.async_inbox, message)
        else:
            await self._async_put_message(self.thread_inbox, message)

    def on_message(self, message):
        if not self._is_deliverable(message):
            LOG.error(f'undeliverable message {message}')
            return
        actor = self.actors[message.dst]
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

    def join(self):
        for t in self.threads:
            t.join()


class ActorContext:
    def __init__(self, executor: ActorExecutor, actor: Actor, state: dict, message: ActorMessage):
        self.executor = executor
        self.registery = executor.registery
        self.actor = actor
        self.state = state
        self.message = message

    def send(self, dst, content=None, dst_node=None):
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
